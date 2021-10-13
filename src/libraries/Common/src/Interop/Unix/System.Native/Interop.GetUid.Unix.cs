// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Runtime.InteropServices;

internal static partial class Interop
{
    internal static partial class Sys
    {
        internal static uint GetEGid() => SysGetEGid();

        internal static uint GetEUid() => SysGetEUid();

        internal static bool IsMemberOfGroup(uint gid)
        {
            if (gid == GetEGid())
            {
                return true;
            }

            uint[]? groups = GetGroups();
            if (groups == null)
            {
                return false;
            }

            return Array.IndexOf(groups, gid) >= 0;
        }
    }
}
