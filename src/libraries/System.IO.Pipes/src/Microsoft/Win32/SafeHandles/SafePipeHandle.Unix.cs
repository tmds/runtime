// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security;
using System.Threading;

namespace Microsoft.Win32.SafeHandles
{
    public sealed partial class SafePipeHandle : SafeHandleZeroOrMinusOneIsInvalid
    {
        private const int DefaultInvalidHandle = -1;
        private static Func<IntPtr, bool, Socket>? s_createSocketForPipe;

        // For anonymous pipes, SafePipeHandle.handle is the file descriptor of the pipe, and the
        // For named pipes, SafePipeHandle.handle is a copy of the file descriptor
        // extracted from the Socket's SafeHandle.
        // This allows operations related to file descriptors to be performed directly on the SafePipeHandle,
        // and operations that should go through the Socket to be done via PipeSocket. We keep the
        // Socket's SafeHandle alive as long as this SafeHandle is alive.

        private Socket? _pipeSocket;
        private SafeHandle? _pipeSocketHandle;
        private volatile int _disposed;

        internal SafePipeHandle(Socket namedPipeSocket) : base(ownsHandle: true)
        {
            SetPipeSocketInterlocked(namedPipeSocket, ownsHandle: true);
            base.SetHandle(_pipeSocketHandle!.DangerousGetHandle());
        }

        internal Socket PipeSocket => _pipeSocket ?? CreatePipeSocket();

        internal SafeHandle? PipeSocketHandle => _pipeSocketHandle;

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing); // must be called before trying to Dispose the socket
            _disposed = 1;
            Socket? socket;
            if (disposing && (socket = Volatile.Read(ref _pipeSocket)) != null)
            {
                socket.Dispose();
                _pipeSocket = null;
            }
        }

        protected override bool ReleaseHandle()
        {
            Debug.Assert(!IsInvalid);

            if (_pipeSocketHandle != null)
            {
                base.SetHandle((IntPtr)DefaultInvalidHandle);
                _pipeSocketHandle.DangerousRelease();
                _pipeSocketHandle = null;
                return true;
            }
            else
            {
                return (long)handle >= 0 ?
                    Interop.Sys.Close(handle) == 0 :
                    true;
            }
        }

        public override bool IsInvalid
        {
            get { return (long)handle < 0 && _pipeSocket == null; }
        }

        private Socket CreatePipeSocket(bool ownsHandle = true)
        {
            Socket? socket = null;
            if (_disposed == 0)
            {
                bool refAdded = false;
                try
                {
                    DangerousAddRef(ref refAdded);

                    Func<IntPtr, bool, Socket> createSocketForPipe = s_createSocketForPipe ??
                        (s_createSocketForPipe = (Func<IntPtr, bool, Socket>)typeof(Socket).GetMethod("CreateForPipeSafeHandle", BindingFlags.Static | BindingFlags.Public)!.CreateDelegate(typeof(Func<IntPtr, bool, Socket>)));
                    socket = SetPipeSocketInterlocked(createSocketForPipe(handle, ownsHandle), ownsHandle);

                    if (_disposed == 1)
                    {
                        Volatile.Write(ref _pipeSocket, null);
                        socket.Dispose();
                        socket = null;
                    }
                }
                finally
                {
                    if (refAdded)
                    {
                        DangerousRelease();
                    }
                }
            }
            return socket ?? throw new ObjectDisposedException(GetType().ToString());;
        }

        private Socket SetPipeSocketInterlocked(Socket socket, bool ownsHandle)
        {
            Debug.Assert(socket != null);

            // Multiple threads may try to create the PipeSocket.
            Socket? current = Interlocked.CompareExchange(ref _pipeSocket, socket, null);
            if (current != null)
            {
                socket.Dispose();
                return current;
            }

            // If we own the handle, defer ownership to the SocketHandle.
            SafeSocketHandle socketHandle = _pipeSocket.SafeHandle;
            if (ownsHandle)
            {
                _pipeSocketHandle = socketHandle;

                bool ignored = false;
                socketHandle.DangerousAddRef(ref ignored);
            }

            return socket;
        }

        internal void SetHandle(IntPtr descriptor, bool ownsHandle = true)
        {
            base.SetHandle(descriptor);

            // Avoid throwing when we own the handle by defering pipe creation.
            if (!ownsHandle)
            {
                _pipeSocket = CreatePipeSocket(ownsHandle);
            }
        }
    }
}
