// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Linq;
using System.Security.Cryptography;
using Xunit;

namespace System.IO.Tests
{
    // Don't run in parallel as the WhenDiskIsFullTheErrorMessageContainsAllDetails test
    // consumes entire available free space on the disk (only on Linux, this is how posix_fallocate works)
    // and if we try to run other disk-writing test in the meantime we are going to get "No space left on device" exception.
    [Collection("NoParallelTests")]
    public partial class FileStream_ctor_options : FileStream_ctor_str_fm_fa_fs_buffer_fo
    {
        protected override string GetExpectedParamName(string paramName) => "value";

        protected override FileStream CreateFileStream(string path, FileMode mode)
            => new FileStream(path,
                    new FileStreamOptions
                    {
                        Mode = mode,
                        Access = mode == FileMode.Append ? FileAccess.Write : FileAccess.ReadWrite
                    });

        protected override FileStream CreateFileStream(string path, FileMode mode, FileAccess access)
            => new FileStream(path,
                    new FileStreamOptions
                    {
                        Mode = mode,
                        Access = access
                    });

        protected override FileStream CreateFileStream(string path, FileMode mode, FileAccess access, FileShare share, int bufferSize, FileOptions options)
            => new FileStream(path,
                    new FileStreamOptions
                    {
                        Mode = mode,
                        Access = access,
                        Share = share,
                        BufferSize = bufferSize,
                        Options = options
                    });
    }

    [CollectionDefinition("NoParallelTests", DisableParallelization = true)]
    public partial class NoParallelTests { }
}
