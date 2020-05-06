// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Win32.SafeHandles;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace System.Net.Sockets
{
    // Note on asynchronous behavior here:

    // The asynchronous socket operations here generally do the following:
    // (1) If the operation queue is Ready (queue is empty), try to perform the operation immediately, non-blocking.
    // If this completes (i.e. does not return EWOULDBLOCK), then we return the results immediately
    // for both success (SocketError.Success) or failure.
    // No callback will happen; callers are expected to handle these synchronous completions themselves.
    // (2) If EWOULDBLOCK is returned, or the queue is not empty, then we enqueue an operation to the
    // appropriate queue and return SocketError.IOPending.
    // Enqueuing itself may fail because the socket is closed before the operation can be enqueued;
    // in this case, we return SocketError.OperationAborted (which matches what Winsock would return in this case).
    // (3) When we receive an epoll notification for the socket, we post a work item to the threadpool
    // to perform the I/O and invoke the callback with the I/O result.

    // Synchronous operations generally do the same, except that instead of returning IOPending,
    // they block on an event handle until the operation is processed by the queue.

    // See comments on OperationQueue below for more details of how the queue coordination works.

    internal sealed class SocketAsyncContext
    {
        // Cached operation instances for operations commonly repeated on the same socket instance,
        // e.g. async accepts, sends/receives with single and multiple buffers.  More can be
        // added in the future if necessary, at the expense of extra fields here.  With a larger
        // refactoring, these could also potentially be moved to SocketAsyncEventArgs, which
        // would be more invasive but which would allow them to be reused across socket instances
        // and also eliminate the interlocked necessary to rent the instances.
        private AcceptOperation? _cachedAcceptOperation;
        private BufferMemoryReceiveOperation? _cachedBufferMemoryReceiveOperation;
        private BufferListReceiveOperation? _cachedBufferListReceiveOperation;
        private BufferMemorySendOperation? _cachedBufferMemorySendOperation;
        private BufferListSendOperation? _cachedBufferListSendOperation;

        private void ReturnOperation(AcceptOperation operation)
        {
            operation.Reset();
            operation.Callback = null;
            operation.SocketAddress = null;
            Volatile.Write(ref _cachedAcceptOperation, operation); // benign race condition
        }

        private void ReturnOperation(BufferMemoryReceiveOperation operation)
        {
            operation.Reset();
            operation.Buffer = default;
            operation.Callback = null;
            operation.SocketAddress = null;
            Volatile.Write(ref _cachedBufferMemoryReceiveOperation, operation); // benign race condition
        }

        private void ReturnOperation(BufferListReceiveOperation operation)
        {
            operation.Reset();
            operation.Buffers = null;
            operation.Callback = null;
            operation.SocketAddress = null;
            Volatile.Write(ref _cachedBufferListReceiveOperation, operation); // benign race condition
        }

        private void ReturnOperation(BufferMemorySendOperation operation)
        {
            operation.Reset();
            operation.Buffer = default;
            operation.Callback = null;
            operation.SocketAddress = null;
            Volatile.Write(ref _cachedBufferMemorySendOperation, operation); // benign race condition
        }

        private void ReturnOperation(BufferListSendOperation operation)
        {
            operation.Reset();
            operation.Buffers = null;
            operation.Callback = null;
            operation.SocketAddress = null;
            Volatile.Write(ref _cachedBufferListSendOperation, operation); // benign race condition
        }

        private AcceptOperation RentAcceptOperation() =>
            Interlocked.Exchange(ref _cachedAcceptOperation, null) ??
            new AcceptOperation(this);

        private BufferMemoryReceiveOperation RentBufferMemoryReceiveOperation() =>
            Interlocked.Exchange(ref _cachedBufferMemoryReceiveOperation, null) ??
            new BufferMemoryReceiveOperation(this);

        private BufferListReceiveOperation RentBufferListReceiveOperation() =>
            Interlocked.Exchange(ref _cachedBufferListReceiveOperation, null) ??
            new BufferListReceiveOperation(this);

        private BufferMemorySendOperation RentBufferMemorySendOperation() =>
            Interlocked.Exchange(ref _cachedBufferMemorySendOperation, null) ??
            new BufferMemorySendOperation(this);

        private BufferListSendOperation RentBufferListSendOperation() =>
            Interlocked.Exchange(ref _cachedBufferListSendOperation, null) ??
            new BufferListSendOperation(this);

        private abstract class AsyncOperation : IThreadPoolWorkItem
        {
            private enum State
            {
                Queued = 0,
                Running = 1,
                Complete = 2,
                Cancelled = 3,
                Pooled = 4
            }

            private int _state; // Actually AsyncOperation.State.

#if DEBUG
            private int _callbackQueued; // When non-zero, the callback has been queued.
#endif

            public readonly SocketAsyncContext AssociatedContext;
            public AsyncOperation? Next = null!; // initialized by helper called from ctor
            protected object? CallbackOrEvent;
            public SocketError ErrorCode;
            public byte[]? SocketAddress;
            public int SocketAddressLen;
            public CancellationTokenRegistration CancellationRegistration;

            public ManualResetEventSlim? Event
            {
                get { return CallbackOrEvent as ManualResetEventSlim; }
                set { CallbackOrEvent = value; }
            }

            public AsyncOperation(SocketAsyncContext context)
            {
                AssociatedContext = context;
                Next = this;
                Reset();
            }

            public void Reset()
            {
                Debug.Assert(Next == this);
                _state = (int)State.Pooled;
#if DEBUG
                _callbackQueued = 0;
#endif
            }

            public bool TryComplete(SocketAsyncContext context)
            {
                TraceWithContext(context, "Enter");

                bool result = DoTryComplete(context);

                TraceWithContext(context, $"Exit, result={result}");

                return result;
            }

            public bool TrySetRunning()
            {
                State oldState = (State)Interlocked.CompareExchange(ref _state, (int)State.Running, (int)State.Queued);
                if (oldState == State.Cancelled)
                {
                    // This operation has already been cancelled, and had its completion processed.
                    // Simply return false to indicate no further processing is needed.
                    return false;
                }

                Debug.Assert(oldState == (int)State.Queued);
                return true;
            }

            public void SetComplete()
            {
                Debug.Assert(Volatile.Read(ref _state) == (int)State.Running);

                Volatile.Write(ref _state, (int)State.Complete);
            }

            public void SetQueued()
            {
                //Debug.Assert(Volatile.Read(ref _state) == (int)State.Running);

                Volatile.Write(ref _state, (int)State.Queued);
            }

            public bool TryCancel(SocketAsyncContext context)
            {
                TraceWithContext(context, "Enter");

                // We're already canceling, so we don't need to still be hooked up to listen to cancellation.
                // The cancellation request could also be caused by something other than the token, so it's
                // important we clean it up, regardless.
                CancellationRegistration.Dispose();

                // Try to transition from Waiting to Cancelled
                SpinWait spinWait = default;
                bool keepWaiting = true;
                while (keepWaiting)
                {
                    int state = Interlocked.CompareExchange(ref _state, (int)State.Cancelled, (int)State.Queued);
                    switch ((State)state)
                    {
                        case State.Running:
                            // A completion attempt is in progress. Keep busy-waiting.
                            TraceWithContext(context, "Busy wait");
                            spinWait.SpinOnce();
                            break;

                        case State.Complete:
                        case State.Pooled:
                            // A completion attempt succeeded. Consider this operation as having completed within the timeout.
                            TraceWithContext(context, "Exit, previously completed");
                            return false;

                        case State.Queued:
                            // This operation was successfully cancelled.
                            // Break out of the loop to handle the cancellation
                            keepWaiting = false;
                            break;

                        case State.Cancelled:
                            // Someone else cancelled the operation.
                            // The previous canceller will have fired the completion, etc.
                            TraceWithContext(context, "Exit, previously cancelled");
                            return false;
                    }
                }

                TraceWithContext(context, "Cancelled, processing completion");

                // The operation successfully cancelled.
                // It's our responsibility to set the error code and queue the completion.
                DoAbort();

                var @event = CallbackOrEvent as ManualResetEventSlim;
                if (@event != null)
                {
                    @event.Set();
                }
                else
                {
#if DEBUG
                    Debug.Assert(Interlocked.CompareExchange(ref _callbackQueued, 1, 0) == 0, $"Unexpected _callbackQueued: {_callbackQueued}");
#endif
                    // We've marked the operation as canceled, and so should invoke the callback, but
                    // we can't pool the object, as ProcessQueue may still have a reference to it, due to
                    // using a pattern whereby it takes the lock to grab an item, but then releases the lock
                    // to do further processing on the item that's still in the list.
                    ThreadPool.UnsafeQueueUserWorkItem(o => ((AsyncOperation)o!).InvokeCallback(allowPooling: false), this);
                }

                TraceWithContext(context, "Exit");

                // Note, we leave the operation in the OperationQueue.
                // When we get around to processing it, we'll see it's cancelled and skip it.
                return true;
            }

            public void Dispatch(ConcurrentQueue<object>? operationQueue = null)
            {
                ManualResetEventSlim? e = Event;
                if (e != null)
                {
                    // Sync operation.  Signal waiting thread to continue processing.
                    e.Set();
                }
                else if (operationQueue != null)
                {
                    operationQueue.Enqueue(this);
                }
                else
                {
                    // Async operation.  Process the IO on the threadpool.
                    ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
                }
            }

            void IThreadPoolWorkItem.Execute()
            {
                // ReadOperation and WriteOperation, the only two types derived from
                // AsyncOperation, implement IThreadPoolWorkItem.Execute to call
                // ProcessAsyncOperation(this) on the appropriate receive or send queue.
                // However, this base class needs to be able to queue them without
                // additional allocation, so it also implements the interface in order
                // to pass the compiler's static checking for the interface, but then
                // when the runtime queries for the interface, it'll use the derived
                // type's interface implementation.  We could instead just make this
                // an abstract and have the derived types override it, but that adds
                // "Execute" as a public method, which could easily be misunderstood.
                // We could also add an abstract method that the base interface implementation
                // invokes, but that adds an extra virtual dispatch.
                Debug.Fail("Expected derived type to implement IThreadPoolWorkItem");
                throw new InvalidOperationException();
            }

            // Called when op is not in the queue yet, so can't be otherwise executing
            public void DoAbort()
            {
                Abort();
                ErrorCode = SocketError.OperationAborted;
            }

            protected abstract void Abort();

            protected abstract bool DoTryComplete(SocketAsyncContext context);

            public abstract void InvokeCallback(bool allowPooling);

            // public void Trace(string message, [CallerMemberName] string? memberName = null)
            // {
            //     OutputTrace($"{IdOf(this)}.{memberName}: {message}");
            // }

            public void TraceWithContext(SocketAsyncContext context, string message, [CallerMemberName] string? memberName = null)
            {
                OutputTrace($"{IdOf(context)}, {IdOf(this)}.{memberName}: {message}");
            }
        }

        // These two abstract classes differentiate the operations that go in the
        // read queue vs the ones that go in the write queue.
        private abstract class ReadOperation : AsyncOperation, IThreadPoolWorkItem
        {
            public ReadOperation(SocketAsyncContext context) : base(context) { }

            void IThreadPoolWorkItem.Execute() => AssociatedContext.ProcessAsyncReadOperation(this);
        }

        private abstract class WriteOperation : AsyncOperation, IThreadPoolWorkItem
        {
            public WriteOperation(SocketAsyncContext context) : base(context) { }

            void IThreadPoolWorkItem.Execute() => AssociatedContext.ProcessAsyncWriteOperation(this);
        }

        private abstract class SendOperation : WriteOperation
        {
            public SocketFlags Flags;
            public int BytesTransferred;
            public int Offset;
            public int Count;

            public SendOperation(SocketAsyncContext context) : base(context) { }

            protected sealed override void Abort() { }

            public Action<int, byte[]?, int, SocketFlags, SocketError>? Callback
            {
                set => CallbackOrEvent = value;
            }

            public override void InvokeCallback(bool allowPooling) =>
                ((Action<int, byte[]?, int, SocketFlags, SocketError>)CallbackOrEvent!)(BytesTransferred, SocketAddress, SocketAddressLen, SocketFlags.None, ErrorCode);
        }

        private sealed class BufferMemorySendOperation : SendOperation
        {
            public Memory<byte> Buffer;

            public BufferMemorySendOperation(SocketAsyncContext context) : base(context) { }

            protected override bool DoTryComplete(SocketAsyncContext context)
            {
                int bufferIndex = 0;
                return SocketPal.TryCompleteSendTo(context._socket, Buffer.Span, null, ref bufferIndex, ref Offset, ref Count, Flags, SocketAddress, SocketAddressLen, ref BytesTransferred, out ErrorCode);
            }

            public override void InvokeCallback(bool allowPooling)
            {
                var cb = (Action<int, byte[]?, int, SocketFlags, SocketError>)CallbackOrEvent!;
                int bt = BytesTransferred;
                byte[]? sa = SocketAddress;
                int sal = SocketAddressLen;
                SocketError ec = ErrorCode;

                if (allowPooling)
                {
                    AssociatedContext.ReturnOperation(this);
                }

                cb(bt, sa, sal, SocketFlags.None, ec);
            }
        }

        private sealed class BufferListSendOperation : SendOperation
        {
            public IList<ArraySegment<byte>>? Buffers;
            public int BufferIndex;

            public BufferListSendOperation(SocketAsyncContext context) : base(context) { }

            protected override bool DoTryComplete(SocketAsyncContext context)
            {
                return SocketPal.TryCompleteSendTo(context._socket, default(ReadOnlySpan<byte>), Buffers, ref BufferIndex, ref Offset, ref Count, Flags, SocketAddress, SocketAddressLen, ref BytesTransferred, out ErrorCode);
            }

            public override void InvokeCallback(bool allowPooling)
            {
                var cb = (Action<int, byte[]?, int, SocketFlags, SocketError>)CallbackOrEvent!;
                int bt = BytesTransferred;
                byte[]? sa = SocketAddress;
                int sal = SocketAddressLen;
                SocketError ec = ErrorCode;

                if (allowPooling)
                {
                    AssociatedContext.ReturnOperation(this);
                }

                cb(bt, sa, sal, SocketFlags.None, ec);
            }
        }

        private sealed unsafe class BufferPtrSendOperation : SendOperation
        {
            public byte* BufferPtr;

            public BufferPtrSendOperation(SocketAsyncContext context) : base(context) { }

            protected override bool DoTryComplete(SocketAsyncContext context)
            {
                int bufferIndex = 0;
                return SocketPal.TryCompleteSendTo(context._socket, new ReadOnlySpan<byte>(BufferPtr, Offset + Count), null, ref bufferIndex, ref Offset, ref Count, Flags, SocketAddress, SocketAddressLen, ref BytesTransferred, out ErrorCode);
            }
        }

        private abstract class ReceiveOperation : ReadOperation
        {
            public SocketFlags Flags;
            public SocketFlags ReceivedFlags;
            public int BytesTransferred;

            public ReceiveOperation(SocketAsyncContext context) : base(context) { }

            protected sealed override void Abort() { }

            public Action<int, byte[]?, int, SocketFlags, SocketError>? Callback
            {
                set => CallbackOrEvent = value;
            }

            public override void InvokeCallback(bool allowPooling) =>
                ((Action<int, byte[]?, int, SocketFlags, SocketError>)CallbackOrEvent!)(
                    BytesTransferred, SocketAddress, SocketAddressLen, ReceivedFlags, ErrorCode);
        }

        private sealed class BufferMemoryReceiveOperation : ReceiveOperation
        {
            public Memory<byte> Buffer;

            public BufferMemoryReceiveOperation(SocketAsyncContext context) : base(context) { }

            protected override bool DoTryComplete(SocketAsyncContext context)
            {
                // Zero byte read is performed to know when data is available.
                // We don't have to call receive, our caller is interested in the event.
                if (Buffer.Length == 0 && Flags == SocketFlags.None && SocketAddress == null)
                {
                    BytesTransferred = 0;
                    ReceivedFlags = SocketFlags.None;
                    ErrorCode = SocketError.Success;
                    return true;
                }
                else
                {
                    return SocketPal.TryCompleteReceiveFrom(context._socket, Buffer.Span, null, Flags, SocketAddress, ref SocketAddressLen, out BytesTransferred, out ReceivedFlags, out ErrorCode);
                }
            }

            public override void InvokeCallback(bool allowPooling)
            {
                var cb = (Action<int, byte[]?, int, SocketFlags, SocketError>)CallbackOrEvent!;
                int bt = BytesTransferred;
                byte[]? sa = SocketAddress;
                int sal = SocketAddressLen;
                SocketFlags rf = ReceivedFlags;
                SocketError ec = ErrorCode;

                if (allowPooling)
                {
                    AssociatedContext.ReturnOperation(this);
                }

                cb(bt, sa, sal, rf, ec);
            }
        }

        private sealed class BufferListReceiveOperation : ReceiveOperation
        {
            public IList<ArraySegment<byte>>? Buffers;

            public BufferListReceiveOperation(SocketAsyncContext context) : base(context) { }

            protected override bool DoTryComplete(SocketAsyncContext context) =>
                SocketPal.TryCompleteReceiveFrom(context._socket, default(Span<byte>), Buffers, Flags, SocketAddress, ref SocketAddressLen, out BytesTransferred, out ReceivedFlags, out ErrorCode);

            public override void InvokeCallback(bool allowPooling)
            {
                var cb = (Action<int, byte[]?, int, SocketFlags, SocketError>)CallbackOrEvent!;
                int bt = BytesTransferred;
                byte[]? sa = SocketAddress;
                int sal = SocketAddressLen;
                SocketFlags rf = ReceivedFlags;
                SocketError ec = ErrorCode;

                if (allowPooling)
                {
                    AssociatedContext.ReturnOperation(this);
                }

                cb(bt, sa, sal, rf, ec);
            }
        }

        private sealed unsafe class BufferPtrReceiveOperation : ReceiveOperation
        {
            public byte* BufferPtr;
            public int Length;

            public BufferPtrReceiveOperation(SocketAsyncContext context) : base(context) { }

            protected override bool DoTryComplete(SocketAsyncContext context) =>
                SocketPal.TryCompleteReceiveFrom(context._socket, new Span<byte>(BufferPtr, Length), null, Flags, SocketAddress, ref SocketAddressLen, out BytesTransferred, out ReceivedFlags, out ErrorCode);
        }

        private sealed class ReceiveMessageFromOperation : ReadOperation
        {
            public Memory<byte> Buffer;
            public SocketFlags Flags;
            public int BytesTransferred;
            public SocketFlags ReceivedFlags;
            public IList<ArraySegment<byte>>? Buffers;

            public bool IsIPv4;
            public bool IsIPv6;
            public IPPacketInformation IPPacketInformation;

            public ReceiveMessageFromOperation(SocketAsyncContext context) : base(context) { }

            protected sealed override void Abort() { }

            public Action<int, byte[], int, SocketFlags, IPPacketInformation, SocketError> Callback
            {
                set => CallbackOrEvent = value;
            }

            protected override bool DoTryComplete(SocketAsyncContext context) =>
                SocketPal.TryCompleteReceiveMessageFrom(context._socket, Buffer.Span, Buffers, Flags, SocketAddress!, ref SocketAddressLen, IsIPv4, IsIPv6, out BytesTransferred, out ReceivedFlags, out IPPacketInformation, out ErrorCode);

            public override void InvokeCallback(bool allowPooling) =>
                ((Action<int, byte[], int, SocketFlags, IPPacketInformation, SocketError>)CallbackOrEvent!)(
                    BytesTransferred, SocketAddress!, SocketAddressLen, ReceivedFlags, IPPacketInformation, ErrorCode);
        }

        private sealed class AcceptOperation : ReadOperation
        {
            public IntPtr AcceptedFileDescriptor;

            public AcceptOperation(SocketAsyncContext context) : base(context) { }

            public Action<IntPtr, byte[], int, SocketError>? Callback
            {
                set => CallbackOrEvent = value;
            }

            protected override void Abort() =>
                AcceptedFileDescriptor = (IntPtr)(-1);

            protected override bool DoTryComplete(SocketAsyncContext context)
            {
                bool completed = SocketPal.TryCompleteAccept(context._socket, SocketAddress!, ref SocketAddressLen, out AcceptedFileDescriptor, out ErrorCode);
                Debug.Assert(ErrorCode == SocketError.Success || AcceptedFileDescriptor == (IntPtr)(-1), $"Unexpected values: ErrorCode={ErrorCode}, AcceptedFileDescriptor={AcceptedFileDescriptor}");
                return completed;
            }

            public override void InvokeCallback(bool allowPooling)
            {
                var cb = (Action<IntPtr, byte[], int, SocketError>)CallbackOrEvent!;
                IntPtr fd = AcceptedFileDescriptor;
                byte[] sa = SocketAddress!;
                int sal = SocketAddressLen;
                SocketError ec = ErrorCode;

                if (allowPooling)
                {
                    AssociatedContext.ReturnOperation(this);
                }

                cb(fd, sa, sal, ec);
            }
        }

        private sealed class ConnectOperation : WriteOperation
        {
            public ConnectOperation(SocketAsyncContext context) : base(context) { }

            public Action<SocketError> Callback
            {
                set => CallbackOrEvent = value;
            }

            protected override void Abort() { }

            protected override bool DoTryComplete(SocketAsyncContext context)
            {
                bool result = SocketPal.TryCompleteConnect(context._socket, SocketAddressLen, out ErrorCode);
                context._socket.RegisterConnectResult(ErrorCode);
                return result;
            }

            public override void InvokeCallback(bool allowPooling) =>
                ((Action<SocketError>)CallbackOrEvent!)(ErrorCode);
        }

        private sealed class SendFileOperation : WriteOperation
        {
            public SafeFileHandle FileHandle = null!; // always set when constructed
            public long Offset;
            public long Count;
            public long BytesTransferred;

            public SendFileOperation(SocketAsyncContext context) : base(context) { }

            protected override void Abort() { }

            public Action<long, SocketError> Callback
            {
                set => CallbackOrEvent = value;
            }

            public override void InvokeCallback(bool allowPooling) =>
                ((Action<long, SocketError>)CallbackOrEvent!)(BytesTransferred, ErrorCode);

            protected override bool DoTryComplete(SocketAsyncContext context) =>
                SocketPal.TryCompleteSendFile(context._socket, FileHandle, ref Offset, ref Count, ref BytesTransferred, out ErrorCode);
        }

        // In debug builds, this struct guards against:
        // (1) Unexpected lock reentrancy, which should never happen
        // (2) Deadlock, by setting a reasonably large timeout
        private readonly struct LockToken : IDisposable
        {
            private readonly object _lockObject;

            public LockToken(object lockObject)
            {
                Debug.Assert(lockObject != null);

                _lockObject = lockObject;

                Debug.Assert(!Monitor.IsEntered(_lockObject));

#if DEBUG
                bool success = Monitor.TryEnter(_lockObject, 10000);
                Debug.Assert(success, "Timed out waiting for queue lock");
#else
                Monitor.Enter(_lockObject);
#endif
            }

            public void Dispose()
            {
                Debug.Assert(Monitor.IsEntered(_lockObject));
                Monitor.Exit(_lockObject);
            }
        }

        private struct OperationQueue<TOperation>
            where TOperation : AsyncOperation
        {
            private int _sequenceNumber;    // This sequence number is updated when we receive an epoll notification.
                                            // It allows us to detect when a new epoll notification has arrived
                                            // since the last time we checked the state of the queue.
                                            // If this happens, we MUST retry the operation, otherwise we risk
                                            // "losing" the notification and causing the operation to pend indefinitely.
            private AsyncOperation? _queue;   // Queue of pending IO operations to process when data becomes available.

            private class AsyncOperationGate : AsyncOperation
            {
                public AsyncOperationGate() : base(null!) { }

                protected override bool DoTryComplete(SocketAsyncContext context)
                {
                    throw new InvalidOperationException();
                }

                public override void InvokeCallback(bool allowPooling)
                {
                    throw new InvalidOperationException();
                }

                protected override void Abort()
                {
                    throw new InvalidOperationException();
                }
            }

            private class SentinelOperation : AsyncOperation
            {
                public SentinelOperation() : base(null!) { }

                protected override bool DoTryComplete(SocketAsyncContext context)
                {
                    throw new InvalidOperationException();
                }

                public override void InvokeCallback(bool allowPooling)
                {
                    throw new InvalidOperationException();
                }

                protected override void Abort()
                {
                    throw new InvalidOperationException();
                }
            }

            private static readonly SentinelOperation DisposedSentinel = new SentinelOperation();

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool IsDisposed(AsyncOperation? queue) => object.ReferenceEquals(queue, DisposedSentinel);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool IsGate(AsyncOperation queue) => queue.GetType() == typeof(AsyncOperationGate);

            public bool IsReady(SocketAsyncContext context, out int observedSequenceNumber)
            {
                observedSequenceNumber = Volatile.Read(ref _sequenceNumber);
                bool isReady = QueueGetFirst() is null;

                Trace(context, $"{isReady}");

                return isReady;
            }

            private AsyncOperation? QueueGetFirst()
            {
                AsyncOperation? queue = Volatile.Read(ref _queue);
                if (queue is null || IsDisposed(queue))
                {
                    return null;
                }

                if (IsGate(queue))
                {
                    lock (queue)
                    {
                        return queue.Next?.Next;
                    }
                }
                else
                {
                    return queue;
                }
            }

            public void Init()
            {
                _sequenceNumber = 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private (bool isFirst, bool isDisposed) Enqueue(AsyncOperation operation)
            {
                Debug.Assert(operation.Next == operation); // non-queued point to self.
                operation.SetQueued(); // TODO this doesn't have to be Volatile
                AsyncOperation? queue = Interlocked.CompareExchange(ref _queue, operation, null);
                if (queue is null)
                {
                    return (true, false);
                }

                return EnqueueSlow(operation, queue);
            }

            private (bool isFirst, bool isDisposed) EnqueueSlow(AsyncOperation operation, AsyncOperation? queue)
            {
                Debug.Assert(queue != null);

                SpinWait spin = default;
                while (true)
                {
                    if (IsDisposed(queue))
                    {
                        return (false, true);
                    }
                    else
                    {
                        // Install a gate.
                        if (!IsGate(queue))
                        {
                            AsyncOperation singleOperation = queue;
                            Debug.Assert(singleOperation.Next == singleOperation);

                            AsyncOperation gate = new AsyncOperationGate();
                            gate.Next = singleOperation;
                            queue = Interlocked.CompareExchange(ref _queue, gate, singleOperation);
                            if (queue != singleOperation)
                            {
                                if (queue is null)
                                {
                                    queue = Interlocked.CompareExchange(ref _queue, operation, null);
                                    if (queue is null)
                                    {
                                        return (true, false);
                                    }
                                }
                                spin.SpinOnce();
                                continue;
                            }
                            queue = gate;
                        }

                        lock (queue)
                        {
                            if (object.ReferenceEquals(_queue, DisposedSentinel))
                            {
                                return (false, true);
                            }

                            AsyncOperation? last = queue.Next;
                            if (last == null) // empty queue
                            {
                                queue.Next = operation;
                                return (true, false);
                            }
                            else
                            {
                                queue.Next = operation;     // gate points to new last
                                operation.Next = last.Next; // new last points to first
                                last.Next = operation;      // previous last points to new last
                                return (false, false);
                            }
                        }
                    }
                }
            }

            // Return true for pending, false for completed synchronously (including failure and abort)
            public bool StartAsyncOperation(SocketAsyncContext context, TOperation operation, int observedSequenceNumber, CancellationToken cancellationToken = default)
            {
                Trace(context, $"Enter");

                if (!context._registered)
                {
                    context.Register();
                }

                (bool isFirst, bool doAbort) = Enqueue(operation);

                if (doAbort)
                {
                    operation.DoAbort();
                    Trace(context, $"Leave, queue stopped");

                    return false;
                }
                else
                {
                    // Now that the object is enqueued, hook up cancellation.
                    // Note that it's possible the call to register itself could
                    // call TryCancel, so we do this after the op is fully enqueued.
                    if (cancellationToken.CanBeCanceled)
                    {
                        operation.CancellationRegistration = cancellationToken.UnsafeRegister(s => ((TOperation)s!).TryCancel(context), operation);
                    }

                    if (isFirst && observedSequenceNumber != Volatile.Read(ref _sequenceNumber))
                    {
                        EnsureProcessingIfNeeded(context);
                    }

                    Trace(context, $"Leave, enqueued {IdOf(operation)}");
                    return true;
                }
            }

            private int _processing;

            private void EnsureProcessingIfNeeded(SocketAsyncContext context, ConcurrentQueue<object>? operationQueue = null)
            {
                Trace(context, $"Enter");

                while (true)
                {
                    AsyncOperation? op;
                    if (Interlocked.CompareExchange(ref _processing, 1, 0) == 0)
                    {
                        op = QueueGetFirst();
                        if (op != null)
                        {
                            Trace(context, "Exit (processing)");

                            // Dispatch processing.
                            op.Dispatch(operationQueue);
                            return;
                        }
                        Volatile.Write(ref _processing, 0);
                    }
                    else
                    {
                        Trace(context, "Exit (already processing)");
                        // Already processing.
                        return;
                    }
                    // Verify no operation was added.
                    op = QueueGetFirst();
                    if (op == null)
                    {
                        Trace(context, "Exit (empty)");
                        return;
                    }
                }
            }

            // Called on the epoll thread whenever we receive an epoll notification.
            public void HandleEvent(SocketAsyncContext context, ConcurrentQueue<object> operationQueue)
            {
                Trace(context, $"Enter");
                Interlocked.Increment(ref _sequenceNumber);
                EnsureProcessingIfNeeded(context, operationQueue);
                Trace(context, $"Leave");
            }

            internal void ProcessAsyncOperation(TOperation op)
            {
                OperationResult result = ProcessQueuedOperation(op);

                Debug.Assert(op.Event == null, "Sync operation encountered in ProcessAsyncOperation");

                if (result == OperationResult.Completed)
                {
                    // At this point, the operation has completed and it's no longer
                    // in the queue / no one else has a reference to it.  We can invoke
                    // the callback and let it pool the object if appropriate. This is
                    // also a good time to unregister from cancellation; we must do
                    // so before the object is returned to the pool (or else a cancellation
                    // request for a previous operation could affect a subsequent one)
                    // and here we know the operation has completed.
                    op.CancellationRegistration.Dispose();
                    op.InvokeCallback(allowPooling: true);
                }
            }

            public enum OperationResult
            {
                Pending = 0,
                Completed = 1,
                Cancelled = 2
            }

            private bool TryStopProcessing(ref int observedSequenceNumber)
            {
                Debug.Assert(Volatile.Read(ref _processing) == 1);

                Volatile.Write(ref _processing, 0);

                int sequenceNumber = Volatile.Read(ref _sequenceNumber);
                if (sequenceNumber == observedSequenceNumber)
                {
                    return true;
                }
                else
                {
                    observedSequenceNumber = sequenceNumber;
                    bool resumeProcessing = Interlocked.CompareExchange(ref _processing, 1, 0) == 0;
                    return !resumeProcessing;
                }
            }

            private void StopProcessing(SocketAsyncContext context)
            {
                Debug.Assert(Volatile.Read(ref _processing) == 1);

                Volatile.Write(ref _processing, 0);

                AsyncOperation? op = QueueGetFirst();
                if (op != null)
                {
                    EnsureProcessingIfNeeded(context);
                }
            }

            private AsyncOperation? DequeueFirstAndGetNext(AsyncOperation first)
            {
                AsyncOperation? queue = Interlocked.CompareExchange(ref _queue, null, first);
                Debug.Assert(queue != null);
                if (object.ReferenceEquals(queue, first) || IsDisposed(queue))
                {
                    return null;
                }

                Debug.Assert(IsGate(queue));
                lock (queue)
                {
                    if (queue.Next == first) // we're the last -> single element
                    {
                        Debug.Assert(first.Next == first); // verify we're a single element list
                        queue.Next = null;
                        return null;
                    }
                    else
                    {
                        AsyncOperation? last = queue.Next;
                        Debug.Assert(last != null); // there is an element
                        Debug.Assert(last.Next == first); // we're first
                        last.Next = first.Next; // skip operation
                        first.Next = first;     // point to self
                        return last.Next;
                    }
                }
            }

            public OperationResult ProcessQueuedOperation(TOperation op)
            {
                Debug.Assert(Volatile.Read(ref _processing) == 1);

                SocketAsyncContext context = op.AssociatedContext;
                Trace(context, $"Enter");

                bool wasCompleted = false;
                while (true)
                {
                    int observedSequenceNumber = Volatile.Read(ref _sequenceNumber);

                    // Try to change the op state to Running.
                    // If this fails, it means the operation was previously cancelled,
                    // and we should just remove it from the queue without further processing.
                    if (!op.TrySetRunning())
                    {
                        break;
                    }

                    // Try to perform the IO
                    if (op.TryComplete(context))
                    {
                        op.SetComplete();
                        wasCompleted = true;
                        break;
                    }

                    op.SetQueued();

                    if (TryStopProcessing(ref observedSequenceNumber))
                    {

                        Trace(context, $"Leave (pending)");
                        return OperationResult.Pending;
                    }
                }

                // Remove the op from the queue and see if there's more to process.

                AsyncOperation? nextOp = DequeueFirstAndGetNext(op);
                if (nextOp != null)
                {
                    Trace(context, $"Leave (dispatch)");
                    nextOp.Dispatch();
                }
                else
                {
                    Trace(context, $"Leave (stop)");
                    StopProcessing(context);
                }

                return (wasCompleted ? OperationResult.Completed : OperationResult.Cancelled);
            }

            private (bool isFirst, bool remaining) RemoveQueued(AsyncOperation operation)
            {
                AsyncOperation? queue = Interlocked.CompareExchange(ref _queue, null, operation);
                if (object.ReferenceEquals(queue, operation))
                {
                    return (isFirst: true, remaining: false);
                }
                if (queue is object && IsGate(queue))
                {
                    lock (queue)
                    {
                        if (queue.Next == operation) // We're the last
                        {
                            if (operation.Next == operation) // We're the only
                            {
                                queue.Next = null; // empty
                                return (isFirst: true, remaining: false);
                            }
                            else
                            {
                                // Find newLast
                                AsyncOperation newLast = operation.Next!;
                                {
                                    AsyncOperation newLastNext = newLast.Next!;
                                    while (newLastNext != operation)
                                    {
                                        newLast = newLastNext;
                                    }
                                }
                                newLast.Next = operation.Next; // last point to first
                                queue.Next = newLast;          // gate points to last
                                operation.Next = operation;    // point to self
                                return (isFirst: false, remaining: true);
                            }
                        }
                        AsyncOperation? last = queue.Next;
                        if (last != null)
                        {
                            AsyncOperation it = last;
                            do
                            {
                                AsyncOperation next = it.Next!;
                                if (next == operation)
                                {
                                    it.Next = operation.Next;   // skip operation
                                    operation.Next = operation; // point to self
                                    return (isFirst: it == last, remaining: true);
                                }
                                it = next;
                            } while (it != last);
                        }
                        return (isFirst: false, remaining: false);
                    }
                }
                return (isFirst: false, remaining: false);
            }

            public void CancelAndContinueProcessing(TOperation op)
            {
                // Note, only sync operations use this method.
                Debug.Assert(op.Event != null);

                (bool isFirst, bool remaining) = RemoveQueued(op);

                if (isFirst && remaining)
                {
                    SocketAsyncContext context = op.AssociatedContext;
                    EnsureProcessingIfNeeded(context);
                }
            }

            // Called when the socket is closed.
            public bool StopAndAbort(SocketAsyncContext context)
            {
                Trace(context, $"Enter");
                bool aborted = false;

                AsyncOperation? queue = Interlocked.Exchange(ref _queue, DisposedSentinel);

                // We should be called exactly once, by SafeSocketHandle.
                Debug.Assert(queue != DisposedSentinel);

                if (queue != null)
                {
                    AsyncOperation? gate = queue as AsyncOperationGate;
                    if (gate != null)
                    {
                        // Synchronize with Enqueue.
                        lock (gate)
                        { }

                        AsyncOperation? last = gate.Next;
                        if (last != null)
                        {
                            AsyncOperation op = last;
                            do
                            {
                                aborted |= op.TryCancel(context);
                                op = op.Next!;
                            } while (op != last);
                        }
                    }
                    else
                    {
                        // queue is single operation
                        aborted |= queue.TryCancel(context);
                    }
                }

                Trace(context, $"Exit");

                return aborted;
            }

            public void Trace(SocketAsyncContext context, string message, [CallerMemberName] string? memberName = null)
            {
                string queueType =
                    typeof(TOperation) == typeof(ReadOperation) ? "recv" :
                    typeof(TOperation) == typeof(WriteOperation) ? "send" :
                    "???";

                OutputTrace($"{IdOf(context)}-{queueType}.{memberName}: {message}, {(_processing == 1 ? "processing" : "not processing")}-{_sequenceNumber}, {((QueueGetFirst() == null) ? "empty" : "not empty")}");
            }
        }

        private readonly SafeSocketHandle _socket;
        private OperationQueue<ReadOperation> _receiveQueue;
        private OperationQueue<WriteOperation> _sendQueue;
        private SocketAsyncEngine.Token _asyncEngineToken;
        private bool _registered;
        private bool _nonBlockingSet;

        private readonly object _registerLock = new object();

        public SocketAsyncContext(SafeSocketHandle socket)
        {
            _socket = socket;

            _receiveQueue.Init();
            _sendQueue.Init();
        }

        private void Register()
        {
            Debug.Assert(_nonBlockingSet);
            lock (_registerLock)
            {
                if (!_registered)
                {
                    Debug.Assert(!_asyncEngineToken.WasAllocated);
                    var token = new SocketAsyncEngine.Token(this);

                    Interop.Error errorCode;
                    if (!token.TryRegister(_socket, out errorCode))
                    {
                        token.Free();
                        if (errorCode == Interop.Error.ENOMEM || errorCode == Interop.Error.ENOSPC)
                        {
                            throw new OutOfMemoryException();
                        }
                        else
                        {
                            throw new InternalException(errorCode);
                        }
                    }

                    _asyncEngineToken = token;
                    _registered = true;

                    Trace("Registered");
                }
            }
        }

        public bool StopAndAbort()
        {
            bool aborted = false;

            // Drain queues
            aborted |= _sendQueue.StopAndAbort(this);
            aborted |= _receiveQueue.StopAndAbort(this);

            lock (_registerLock)
            {
                // Freeing the token will prevent any future event delivery.  This socket will be unregistered
                // from the event port automatically by the OS when it's closed.
                _asyncEngineToken.Free();
            }

            return aborted;
        }

        public void SetNonBlocking()
        {
            //
            // Our sockets may start as blocking, and later transition to non-blocking, either because the user
            // explicitly requested non-blocking mode, or because we need non-blocking mode to support async
            // operations.  We never transition back to blocking mode, to avoid problems synchronizing that
            // transition with the async infrastructure.
            //
            // Note that there's no synchronization here, so we may set the non-blocking option multiple times
            // in a race.  This should be fine.
            //
            if (!_nonBlockingSet)
            {
                if (Interop.Sys.Fcntl.SetIsNonBlocking(_socket, 1) != 0)
                {
                    throw new SocketException((int)SocketPal.GetSocketErrorForErrorCode(Interop.Sys.GetLastError()));
                }

                _nonBlockingSet = true;
            }
        }

        private void PerformSyncOperation<TOperation>(ref OperationQueue<TOperation> queue, TOperation operation, int timeout, int observedSequenceNumber)
            where TOperation : AsyncOperation
        {
            Debug.Assert(timeout == -1 || timeout > 0, $"Unexpected timeout: {timeout}");

            using (var e = new ManualResetEventSlim(false, 0))
            {
                operation.Event = e;

                if (!queue.StartAsyncOperation(this, operation, observedSequenceNumber))
                {
                    // Completed synchronously
                    return;
                }

                bool timeoutExpired = false;
                while (true)
                {
                    DateTime waitStart = DateTime.UtcNow;

                    if (!e.Wait(timeout))
                    {
                        timeoutExpired = true;
                        break;
                    }

                    // Reset the event now to avoid lost notifications if the processing is unsuccessful.
                    e.Reset();

                    // We've been signalled to try to process the operation.
                    OperationQueue<TOperation>.OperationResult result = queue.ProcessQueuedOperation(operation);
                    if (result == OperationQueue<TOperation>.OperationResult.Completed ||
                        result == OperationQueue<TOperation>.OperationResult.Cancelled)
                    {
                        break;
                    }

                    // Couldn't process the operation.
                    // Adjust timeout and try again.
                    if (timeout > 0)
                    {
                        timeout -= (DateTime.UtcNow - waitStart).Milliseconds;

                        if (timeout <= 0)
                        {
                            timeoutExpired = true;
                            break;
                        }
                    }
                }

                if (timeoutExpired)
                {
                    queue.CancelAndContinueProcessing(operation);
                    operation.ErrorCode = SocketError.TimedOut;
                }
            }
        }

        private bool ShouldRetrySyncOperation(out SocketError errorCode)
        {
            if (_nonBlockingSet)
            {
                errorCode = SocketError.Success;    // Will be ignored
                return true;
            }

            // We are in blocking mode, so the EAGAIN we received indicates a timeout.
            errorCode = SocketError.TimedOut;
            return false;
        }

        private void ProcessAsyncReadOperation(ReadOperation op) => _receiveQueue.ProcessAsyncOperation(op);

        private void ProcessAsyncWriteOperation(WriteOperation op) => _sendQueue.ProcessAsyncOperation(op);

        public SocketError Accept(byte[] socketAddress, ref int socketAddressLen, out IntPtr acceptedFd)
        {
            Debug.Assert(socketAddress != null, "Expected non-null socketAddress");
            Debug.Assert(socketAddressLen > 0, $"Unexpected socketAddressLen: {socketAddressLen}");

            SocketError errorCode;
            int observedSequenceNumber;
            if (_receiveQueue.IsReady(this, out observedSequenceNumber) &&
                SocketPal.TryCompleteAccept(_socket, socketAddress, ref socketAddressLen, out acceptedFd, out errorCode))
            {
                Debug.Assert(errorCode == SocketError.Success || acceptedFd == (IntPtr)(-1), $"Unexpected values: errorCode={errorCode}, acceptedFd={acceptedFd}");
                return errorCode;
            }

            var operation = new AcceptOperation(this)
            {
                SocketAddress = socketAddress,
                SocketAddressLen = socketAddressLen,
            };

            PerformSyncOperation(ref _receiveQueue, operation, -1, observedSequenceNumber);

            socketAddressLen = operation.SocketAddressLen;
            acceptedFd = operation.AcceptedFileDescriptor;
            return operation.ErrorCode;
        }

        public SocketError AcceptAsync(byte[] socketAddress, ref int socketAddressLen, out IntPtr acceptedFd, Action<IntPtr, byte[], int, SocketError> callback)
        {
            Debug.Assert(socketAddress != null, "Expected non-null socketAddress");
            Debug.Assert(socketAddressLen > 0, $"Unexpected socketAddressLen: {socketAddressLen}");
            Debug.Assert(callback != null, "Expected non-null callback");

            SetNonBlocking();

            SocketError errorCode;
            int observedSequenceNumber;
            if (_receiveQueue.IsReady(this, out observedSequenceNumber) &&
                SocketPal.TryCompleteAccept(_socket, socketAddress, ref socketAddressLen, out acceptedFd, out errorCode))
            {
                Debug.Assert(errorCode == SocketError.Success || acceptedFd == (IntPtr)(-1), $"Unexpected values: errorCode={errorCode}, acceptedFd={acceptedFd}");

                return errorCode;
            }

            AcceptOperation operation = RentAcceptOperation();
            operation.Callback = callback;
            operation.SocketAddress = socketAddress;
            operation.SocketAddressLen = socketAddressLen;

            if (!_receiveQueue.StartAsyncOperation(this, operation, observedSequenceNumber))
            {
                socketAddressLen = operation.SocketAddressLen;
                acceptedFd = operation.AcceptedFileDescriptor;
                errorCode = operation.ErrorCode;

                ReturnOperation(operation);
                return errorCode;
            }

            acceptedFd = (IntPtr)(-1);
            return SocketError.IOPending;
        }

        public SocketError Connect(byte[] socketAddress, int socketAddressLen)
        {
            Debug.Assert(socketAddress != null, "Expected non-null socketAddress");
            Debug.Assert(socketAddressLen > 0, $"Unexpected socketAddressLen: {socketAddressLen}");

            // Connect is different than the usual "readiness" pattern of other operations.
            // We need to call TryStartConnect to initiate the connect with the OS,
            // before we try to complete it via epoll notification.
            // Thus, always call TryStartConnect regardless of readiness.
            SocketError errorCode;
            int observedSequenceNumber;
            _sendQueue.IsReady(this, out observedSequenceNumber);
            if (SocketPal.TryStartConnect(_socket, socketAddress, socketAddressLen, out errorCode))
            {
                _socket.RegisterConnectResult(errorCode);
                return errorCode;
            }

            var operation = new ConnectOperation(this)
            {
                SocketAddress = socketAddress,
                SocketAddressLen = socketAddressLen
            };

            PerformSyncOperation(ref _sendQueue, operation, -1, observedSequenceNumber);

            return operation.ErrorCode;
        }

        public SocketError ConnectAsync(byte[] socketAddress, int socketAddressLen, Action<SocketError> callback)
        {
            Debug.Assert(socketAddress != null, "Expected non-null socketAddress");
            Debug.Assert(socketAddressLen > 0, $"Unexpected socketAddressLen: {socketAddressLen}");
            Debug.Assert(callback != null, "Expected non-null callback");

            SetNonBlocking();

            // Connect is different than the usual "readiness" pattern of other operations.
            // We need to initiate the connect before we try to complete it.
            // Thus, always call TryStartConnect regardless of readiness.
            SocketError errorCode;
            int observedSequenceNumber;
            _sendQueue.IsReady(this, out observedSequenceNumber);
            if (SocketPal.TryStartConnect(_socket, socketAddress, socketAddressLen, out errorCode))
            {
                _socket.RegisterConnectResult(errorCode);
                return errorCode;
            }

            var operation = new ConnectOperation(this)
            {
                Callback = callback,
                SocketAddress = socketAddress,
                SocketAddressLen = socketAddressLen
            };

            if (!_sendQueue.StartAsyncOperation(this, operation, observedSequenceNumber))
            {
                return operation.ErrorCode;
            }

            return SocketError.IOPending;
        }

        public SocketError Receive(Memory<byte> buffer, ref SocketFlags flags, int timeout, out int bytesReceived)
        {
            int socketAddressLen = 0;
            return ReceiveFrom(buffer, ref flags, null, ref socketAddressLen, timeout, out bytesReceived);
        }

        public SocketError Receive(Span<byte> buffer, ref SocketFlags flags, int timeout, out int bytesReceived)
        {
            int socketAddressLen = 0;
            return ReceiveFrom(buffer, ref flags, null, ref socketAddressLen, timeout, out bytesReceived);
        }

        public SocketError ReceiveAsync(Memory<byte> buffer, SocketFlags flags, out int bytesReceived, out SocketFlags receivedFlags, Action<int, byte[]?, int, SocketFlags, SocketError> callback, CancellationToken cancellationToken)
        {
            int socketAddressLen = 0;
            return ReceiveFromAsync(buffer, flags, null, ref socketAddressLen, out bytesReceived, out receivedFlags, callback, cancellationToken);
        }

        public SocketError ReceiveFrom(Memory<byte> buffer, ref SocketFlags flags, byte[]? socketAddress, ref int socketAddressLen, int timeout, out int bytesReceived)
        {
            Debug.Assert(timeout == -1 || timeout > 0, $"Unexpected timeout: {timeout}");

            SocketFlags receivedFlags;
            SocketError errorCode;
            int observedSequenceNumber;
            if (_receiveQueue.IsReady(this, out observedSequenceNumber) &&
                (SocketPal.TryCompleteReceiveFrom(_socket, buffer.Span, flags, socketAddress, ref socketAddressLen, out bytesReceived, out receivedFlags, out errorCode) ||
                !ShouldRetrySyncOperation(out errorCode)))
            {
                flags = receivedFlags;
                return errorCode;
            }

            var operation = new BufferMemoryReceiveOperation(this)
            {
                Buffer = buffer,
                Flags = flags,
                SocketAddress = socketAddress,
                SocketAddressLen = socketAddressLen,
            };

            PerformSyncOperation(ref _receiveQueue, operation, timeout, observedSequenceNumber);

            flags = operation.ReceivedFlags;
            bytesReceived = operation.BytesTransferred;
            return operation.ErrorCode;
        }

        public unsafe SocketError ReceiveFrom(Span<byte> buffer, ref SocketFlags flags, byte[]? socketAddress, ref int socketAddressLen, int timeout, out int bytesReceived)
        {
            SocketFlags receivedFlags;
            SocketError errorCode;
            int observedSequenceNumber;
            if (_receiveQueue.IsReady(this, out observedSequenceNumber) &&
                (SocketPal.TryCompleteReceiveFrom(_socket, buffer, flags, socketAddress, ref socketAddressLen, out bytesReceived, out receivedFlags, out errorCode) ||
                !ShouldRetrySyncOperation(out errorCode)))
            {
                flags = receivedFlags;
                return errorCode;
            }

            fixed (byte* bufferPtr = &MemoryMarshal.GetReference(buffer))
            {
                var operation = new BufferPtrReceiveOperation(this)
                {
                    BufferPtr = bufferPtr,
                    Length = buffer.Length,
                    Flags = flags,
                    SocketAddress = socketAddress,
                    SocketAddressLen = socketAddressLen,
                };

                PerformSyncOperation(ref _receiveQueue, operation, timeout, observedSequenceNumber);

                flags = operation.ReceivedFlags;
                bytesReceived = operation.BytesTransferred;
                return operation.ErrorCode;
            }
        }

        public SocketError ReceiveFromAsync(Memory<byte> buffer,  SocketFlags flags, byte[]? socketAddress, ref int socketAddressLen, out int bytesReceived, out SocketFlags receivedFlags, Action<int, byte[]?, int, SocketFlags, SocketError> callback, CancellationToken cancellationToken = default)
        {
            SetNonBlocking();

            SocketError errorCode;
            int observedSequenceNumber;
            if (_receiveQueue.IsReady(this, out observedSequenceNumber) &&
                SocketPal.TryCompleteReceiveFrom(_socket, buffer.Span, flags, socketAddress, ref socketAddressLen, out bytesReceived, out receivedFlags, out errorCode))
            {
                return errorCode;
            }

            BufferMemoryReceiveOperation operation = RentBufferMemoryReceiveOperation();
            operation.Callback = callback;
            operation.Buffer = buffer;
            operation.Flags = flags;
            operation.SocketAddress = socketAddress;
            operation.SocketAddressLen = socketAddressLen;

            if (!_receiveQueue.StartAsyncOperation(this, operation, observedSequenceNumber, cancellationToken))
            {
                receivedFlags = operation.ReceivedFlags;
                bytesReceived = operation.BytesTransferred;
                errorCode = operation.ErrorCode;

                ReturnOperation(operation);
                return errorCode;
            }

            bytesReceived = 0;
            receivedFlags = SocketFlags.None;
            return SocketError.IOPending;
        }

        public SocketError Receive(IList<ArraySegment<byte>> buffers, ref SocketFlags flags, int timeout, out int bytesReceived)
        {
            return ReceiveFrom(buffers, ref flags, null, 0, timeout, out bytesReceived);
        }

        public SocketError ReceiveAsync(IList<ArraySegment<byte>> buffers, SocketFlags flags, out int bytesReceived, out SocketFlags receivedFlags, Action<int, byte[]?, int, SocketFlags, SocketError> callback)
        {
            int socketAddressLen = 0;
            return ReceiveFromAsync(buffers, flags, null, ref socketAddressLen, out bytesReceived, out receivedFlags, callback);
        }

        public SocketError ReceiveFrom(IList<ArraySegment<byte>> buffers, ref SocketFlags flags, byte[]? socketAddress, int socketAddressLen, int timeout, out int bytesReceived)
        {
            Debug.Assert(timeout == -1 || timeout > 0, $"Unexpected timeout: {timeout}");

            SocketFlags receivedFlags;
            SocketError errorCode;
            int observedSequenceNumber;
            if (_receiveQueue.IsReady(this, out observedSequenceNumber) &&
                (SocketPal.TryCompleteReceiveFrom(_socket, buffers, flags, socketAddress, ref socketAddressLen, out bytesReceived, out receivedFlags, out errorCode) ||
                !ShouldRetrySyncOperation(out errorCode)))
            {
                flags = receivedFlags;
                return errorCode;
            }

            var operation = new BufferListReceiveOperation(this)
            {
                Buffers = buffers,
                Flags = flags,
                SocketAddress = socketAddress,
                SocketAddressLen = socketAddressLen
            };

            PerformSyncOperation(ref _receiveQueue, operation, timeout, observedSequenceNumber);

            socketAddressLen = operation.SocketAddressLen;
            flags = operation.ReceivedFlags;
            bytesReceived = operation.BytesTransferred;
            return operation.ErrorCode;
        }

        public SocketError ReceiveFromAsync(IList<ArraySegment<byte>> buffers, SocketFlags flags, byte[]? socketAddress, ref int socketAddressLen, out int bytesReceived, out SocketFlags receivedFlags, Action<int, byte[]?, int, SocketFlags, SocketError> callback)
        {
            SetNonBlocking();

            SocketError errorCode;
            int observedSequenceNumber;
            if (_receiveQueue.IsReady(this, out observedSequenceNumber) &&
                SocketPal.TryCompleteReceiveFrom(_socket, buffers, flags, socketAddress, ref socketAddressLen, out bytesReceived, out receivedFlags, out errorCode))
            {
                // Synchronous success or failure
                return errorCode;
            }

            BufferListReceiveOperation operation = RentBufferListReceiveOperation();
            operation.Callback = callback;
            operation.Buffers = buffers;
            operation.Flags = flags;
            operation.SocketAddress = socketAddress;
            operation.SocketAddressLen = socketAddressLen;

            if (!_receiveQueue.StartAsyncOperation(this, operation, observedSequenceNumber))
            {
                socketAddressLen = operation.SocketAddressLen;
                receivedFlags = operation.ReceivedFlags;
                bytesReceived = operation.BytesTransferred;
                errorCode = operation.ErrorCode;

                ReturnOperation(operation);
                return errorCode;
            }

            receivedFlags = SocketFlags.None;
            bytesReceived = 0;
            return SocketError.IOPending;
        }

        public SocketError ReceiveMessageFrom(
            Memory<byte> buffer, IList<ArraySegment<byte>>? buffers, ref SocketFlags flags, byte[] socketAddress, ref int socketAddressLen, bool isIPv4, bool isIPv6, int timeout, out IPPacketInformation ipPacketInformation, out int bytesReceived)
        {
            Debug.Assert(timeout == -1 || timeout > 0, $"Unexpected timeout: {timeout}");

            SocketFlags receivedFlags;
            SocketError errorCode;
            int observedSequenceNumber;
            if (_receiveQueue.IsReady(this, out observedSequenceNumber) &&
                (SocketPal.TryCompleteReceiveMessageFrom(_socket, buffer.Span, buffers, flags, socketAddress, ref socketAddressLen, isIPv4, isIPv6, out bytesReceived, out receivedFlags, out ipPacketInformation, out errorCode) ||
                !ShouldRetrySyncOperation(out errorCode)))
            {
                flags = receivedFlags;
                return errorCode;
            }

            var operation = new ReceiveMessageFromOperation(this)
            {
                Buffer = buffer,
                Buffers = buffers,
                Flags = flags,
                SocketAddress = socketAddress,
                SocketAddressLen = socketAddressLen,
                IsIPv4 = isIPv4,
                IsIPv6 = isIPv6,
            };

            PerformSyncOperation(ref _receiveQueue, operation, timeout, observedSequenceNumber);

            socketAddressLen = operation.SocketAddressLen;
            flags = operation.ReceivedFlags;
            ipPacketInformation = operation.IPPacketInformation;
            bytesReceived = operation.BytesTransferred;
            return operation.ErrorCode;
        }

        public SocketError ReceiveMessageFromAsync(Memory<byte> buffer, IList<ArraySegment<byte>>? buffers, SocketFlags flags, byte[] socketAddress, ref int socketAddressLen, bool isIPv4, bool isIPv6, out int bytesReceived, out SocketFlags receivedFlags, out IPPacketInformation ipPacketInformation, Action<int, byte[], int, SocketFlags, IPPacketInformation, SocketError> callback)
        {
            SetNonBlocking();

            SocketError errorCode;
            int observedSequenceNumber;
            if (_receiveQueue.IsReady(this, out observedSequenceNumber) &&
                SocketPal.TryCompleteReceiveMessageFrom(_socket, buffer.Span, buffers, flags, socketAddress, ref socketAddressLen, isIPv4, isIPv6, out bytesReceived, out receivedFlags, out ipPacketInformation, out errorCode))
            {
                return errorCode;
            }

            var operation = new ReceiveMessageFromOperation(this)
            {
                Callback = callback,
                Buffer = buffer,
                Buffers = buffers,
                Flags = flags,
                SocketAddress = socketAddress,
                SocketAddressLen = socketAddressLen,
                IsIPv4 = isIPv4,
                IsIPv6 = isIPv6,
            };

            if (!_receiveQueue.StartAsyncOperation(this, operation, observedSequenceNumber))
            {
                socketAddressLen = operation.SocketAddressLen;
                receivedFlags = operation.ReceivedFlags;
                ipPacketInformation = operation.IPPacketInformation;
                bytesReceived = operation.BytesTransferred;
                return operation.ErrorCode;
            }

            ipPacketInformation = default(IPPacketInformation);
            bytesReceived = 0;
            receivedFlags = SocketFlags.None;
            return SocketError.IOPending;
        }

        public SocketError Send(ReadOnlySpan<byte> buffer, SocketFlags flags, int timeout, out int bytesSent) =>
            SendTo(buffer, flags, null, 0, timeout, out bytesSent);

        public SocketError Send(byte[] buffer, int offset, int count, SocketFlags flags, int timeout, out int bytesSent)
        {
            return SendTo(buffer, offset, count, flags, null, 0, timeout, out bytesSent);
        }

        public SocketError SendAsync(Memory<byte> buffer, int offset, int count, SocketFlags flags, out int bytesSent, Action<int, byte[]?, int, SocketFlags, SocketError> callback, CancellationToken cancellationToken)
        {
            int socketAddressLen = 0;
            return SendToAsync(buffer, offset, count, flags, null, ref socketAddressLen, out bytesSent, callback, cancellationToken);
        }

        public SocketError SendTo(byte[] buffer, int offset, int count, SocketFlags flags, byte[]? socketAddress, int socketAddressLen, int timeout, out int bytesSent)
        {
            Debug.Assert(timeout == -1 || timeout > 0, $"Unexpected timeout: {timeout}");

            bytesSent = 0;
            SocketError errorCode;
            int observedSequenceNumber;
            if (_sendQueue.IsReady(this, out observedSequenceNumber) &&
                (SocketPal.TryCompleteSendTo(_socket, buffer, ref offset, ref count, flags, socketAddress, socketAddressLen, ref bytesSent, out errorCode) ||
                !ShouldRetrySyncOperation(out errorCode)))
            {
                return errorCode;
            }

            var operation = new BufferMemorySendOperation(this)
            {
                Buffer = buffer,
                Offset = offset,
                Count = count,
                Flags = flags,
                SocketAddress = socketAddress,
                SocketAddressLen = socketAddressLen,
                BytesTransferred = bytesSent
            };

            PerformSyncOperation(ref _sendQueue, operation, timeout, observedSequenceNumber);

            bytesSent = operation.BytesTransferred;
            return operation.ErrorCode;
        }

        public unsafe SocketError SendTo(ReadOnlySpan<byte> buffer, SocketFlags flags, byte[]? socketAddress, int socketAddressLen, int timeout, out int bytesSent)
        {
            Debug.Assert(timeout == -1 || timeout > 0, $"Unexpected timeout: {timeout}");

            bytesSent = 0;
            SocketError errorCode;
            int bufferIndexIgnored = 0, offset = 0, count = buffer.Length;
            int observedSequenceNumber;
            if (_sendQueue.IsReady(this, out observedSequenceNumber) &&
                (SocketPal.TryCompleteSendTo(_socket, buffer, null, ref bufferIndexIgnored, ref offset, ref count, flags, socketAddress, socketAddressLen, ref bytesSent, out errorCode) ||
                !ShouldRetrySyncOperation(out errorCode)))
            {
                return errorCode;
            }

            fixed (byte* bufferPtr = &MemoryMarshal.GetReference(buffer))
            {
                var operation = new BufferPtrSendOperation(this)
                {
                    BufferPtr = bufferPtr,
                    Offset = offset,
                    Count = count,
                    Flags = flags,
                    SocketAddress = socketAddress,
                    SocketAddressLen = socketAddressLen,
                    BytesTransferred = bytesSent
                };

                PerformSyncOperation(ref _sendQueue, operation, timeout, observedSequenceNumber);

                bytesSent = operation.BytesTransferred;
                return operation.ErrorCode;
            }
        }

        public SocketError SendToAsync(Memory<byte> buffer, int offset, int count, SocketFlags flags, byte[]? socketAddress, ref int socketAddressLen, out int bytesSent, Action<int, byte[]?, int, SocketFlags, SocketError> callback, CancellationToken cancellationToken = default)
        {
            SetNonBlocking();

            bytesSent = 0;
            SocketError errorCode;
            int observedSequenceNumber;
            if (_sendQueue.IsReady(this, out observedSequenceNumber) &&
                SocketPal.TryCompleteSendTo(_socket, buffer.Span, ref offset, ref count, flags, socketAddress, socketAddressLen, ref bytesSent, out errorCode))
            {
                return errorCode;
            }

            BufferMemorySendOperation operation = RentBufferMemorySendOperation();
            operation.Callback = callback;
            operation.Buffer = buffer;
            operation.Offset = offset;
            operation.Count = count;
            operation.Flags = flags;
            operation.SocketAddress = socketAddress;
            operation.SocketAddressLen = socketAddressLen;
            operation.BytesTransferred = bytesSent;

            if (!_sendQueue.StartAsyncOperation(this, operation, observedSequenceNumber, cancellationToken))
            {
                bytesSent = operation.BytesTransferred;
                errorCode = operation.ErrorCode;

                ReturnOperation(operation);
                return errorCode;
            }

            return SocketError.IOPending;
        }

        public SocketError Send(IList<ArraySegment<byte>> buffers, SocketFlags flags, int timeout, out int bytesSent)
        {
            return SendTo(buffers, flags, null, 0, timeout, out bytesSent);
        }

        public SocketError SendAsync(IList<ArraySegment<byte>> buffers, SocketFlags flags, out int bytesSent, Action<int, byte[]?, int, SocketFlags, SocketError> callback)
        {
            int socketAddressLen = 0;
            return SendToAsync(buffers, flags, null, ref socketAddressLen, out bytesSent, callback);
        }

        public SocketError SendTo(IList<ArraySegment<byte>> buffers, SocketFlags flags, byte[]? socketAddress, int socketAddressLen, int timeout, out int bytesSent)
        {
            Debug.Assert(timeout == -1 || timeout > 0, $"Unexpected timeout: {timeout}");

            bytesSent = 0;
            int bufferIndex = 0;
            int offset = 0;
            SocketError errorCode;
            int observedSequenceNumber;
            if (_sendQueue.IsReady(this, out observedSequenceNumber) &&
                (SocketPal.TryCompleteSendTo(_socket, buffers, ref bufferIndex, ref offset, flags, socketAddress, socketAddressLen, ref bytesSent, out errorCode) ||
                !ShouldRetrySyncOperation(out errorCode)))
            {
                return errorCode;
            }

            var operation = new BufferListSendOperation(this)
            {
                Buffers = buffers,
                BufferIndex = bufferIndex,
                Offset = offset,
                Flags = flags,
                SocketAddress = socketAddress,
                SocketAddressLen = socketAddressLen,
                BytesTransferred = bytesSent
            };

            PerformSyncOperation(ref _sendQueue, operation, timeout, observedSequenceNumber);

            bytesSent = operation.BytesTransferred;
            return operation.ErrorCode;
        }

        public SocketError SendToAsync(IList<ArraySegment<byte>> buffers, SocketFlags flags, byte[]? socketAddress, ref int socketAddressLen, out int bytesSent, Action<int, byte[]?, int, SocketFlags, SocketError> callback)
        {
            SetNonBlocking();

            bytesSent = 0;
            int bufferIndex = 0;
            int offset = 0;
            SocketError errorCode;
            int observedSequenceNumber;
            if (_sendQueue.IsReady(this, out observedSequenceNumber) &&
                SocketPal.TryCompleteSendTo(_socket, buffers, ref bufferIndex, ref offset, flags, socketAddress, socketAddressLen, ref bytesSent, out errorCode))
            {
                return errorCode;
            }

            BufferListSendOperation operation = RentBufferListSendOperation();
            operation.Callback = callback;
            operation.Buffers = buffers;
            operation.BufferIndex = bufferIndex;
            operation.Offset = offset;
            operation.Flags = flags;
            operation.SocketAddress = socketAddress;
            operation.SocketAddressLen = socketAddressLen;
            operation.BytesTransferred = bytesSent;

            if (!_sendQueue.StartAsyncOperation(this, operation, observedSequenceNumber))
            {
                bytesSent = operation.BytesTransferred;
                errorCode = operation.ErrorCode;

                ReturnOperation(operation);
                return errorCode;
            }

            return SocketError.IOPending;
        }

        public SocketError SendFile(SafeFileHandle fileHandle, long offset, long count, int timeout, out long bytesSent)
        {
            Debug.Assert(timeout == -1 || timeout > 0, $"Unexpected timeout: {timeout}");

            bytesSent = 0;
            SocketError errorCode;
            int observedSequenceNumber;
            if (_sendQueue.IsReady(this, out observedSequenceNumber) &&
                (SocketPal.TryCompleteSendFile(_socket, fileHandle, ref offset, ref count, ref bytesSent, out errorCode) ||
                !ShouldRetrySyncOperation(out errorCode)))
            {
                return errorCode;
            }

            var operation = new SendFileOperation(this)
            {
                FileHandle = fileHandle,
                Offset = offset,
                Count = count,
                BytesTransferred = bytesSent
            };

            PerformSyncOperation(ref _sendQueue, operation, timeout, observedSequenceNumber);

            bytesSent = operation.BytesTransferred;
            return operation.ErrorCode;
        }

        public SocketError SendFileAsync(SafeFileHandle fileHandle, long offset, long count, out long bytesSent, Action<long, SocketError> callback)
        {
            SetNonBlocking();

            bytesSent = 0;
            SocketError errorCode;
            int observedSequenceNumber;
            if (_sendQueue.IsReady(this, out observedSequenceNumber) &&
                SocketPal.TryCompleteSendFile(_socket, fileHandle, ref offset, ref count, ref bytesSent, out errorCode))
            {
                return errorCode;
            }

            var operation = new SendFileOperation(this)
            {
                Callback = callback,
                FileHandle = fileHandle,
                Offset = offset,
                Count = count,
                BytesTransferred = bytesSent
            };

            if (!_sendQueue.StartAsyncOperation(this, operation, observedSequenceNumber))
            {
                bytesSent = operation.BytesTransferred;
                return operation.ErrorCode;
            }

            return SocketError.IOPending;
        }

        public unsafe void HandleEvents(Interop.Sys.SocketEvents events, ConcurrentQueue<object> operationQueue)
        {
            if ((events & Interop.Sys.SocketEvents.Error) != 0)
            {
                // Set the Read and Write flags as well; the processing for these events
                // will pick up the error.
                events |= Interop.Sys.SocketEvents.Read | Interop.Sys.SocketEvents.Write;
            }

            if ((events & Interop.Sys.SocketEvents.Read) != 0)
            {
                _receiveQueue.HandleEvent(this, operationQueue);
            }

            if ((events & Interop.Sys.SocketEvents.Write) != 0)
            {
                _sendQueue.HandleEvent(this, operationQueue);
            }
        }

        //
        // Tracing stuff
        //

        // To enabled tracing:
        // (1) Add reference to System.Console in the csproj
        // (2) #define SOCKETASYNCCONTEXT_TRACE

        public void Trace(string message, [CallerMemberName] string? memberName = null)
        {
            OutputTrace($"{IdOf(this)}.{memberName}: {message}");
        }


        public static void OutputTrace(string s)
        {
            // CONSIDER: Change to NetEventSource
            Console.WriteLine(s);
        }

        public static string IdOf(object o) => o == null ? "(null)" : $"{o.GetType().Name}#{o.GetHashCode():X2}";
    }
}
