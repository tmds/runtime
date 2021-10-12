// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Microsoft.Win32.SafeHandles;

namespace System.IO.Strategies
{
    internal sealed class SyncWindowsFileStreamStrategy : OSFileStreamStrategy
    {
        internal SyncWindowsFileStreamStrategy(SafeFileHandle handle, FileAccess access) : base(handle, access)
        {
        }

        internal SyncWindowsFileStreamStrategy(string path, FileMode mode, FileAccess access, FileShare share, FileOptions options)
            : base(path, mode, access, share, options)
        {
        }

        internal override bool IsAsync => false;
    }
}
