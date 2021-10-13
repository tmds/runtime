// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Runtime.InteropServices;
using System.Diagnostics;

internal static partial class Interop
{
    internal static partial class Sys
    {
        private const uint PrivilegedId = 0;
        private const uint InvalidId = unchecked((uint)(-1));

        // Cached values for egid, euid and groups.
        // When the user can change euid, s_egid is set to PrivilegedId, and the cache is not used.
        // When the user can change egid/groups, s_egid is set to PrivilegedId, and the cache is not used.
        private static uint s_egid = InvalidId;
        private static uint s_euid = InvalidId;
        private static volatile uint[]? s_groups; // volatile to ensure the array is sorted before it's assigned.

        internal static uint GetEGid()
        {
            if (s_egid == InvalidId)
            {
                // When we can't determine the user is priviledged to change the id,
                // assume he is to avoid caching.
                if (!procfs.TryGetPermittedCapabilities(out procfs.Capabilities capabilities) ||
                    ((capabilities & procfs.Capabilities.CAP_SETGID) != 0))
                {
                    s_egid = PrivilegedId;
                }
                else
                {
                    s_egid = SysGetEGid();
                }
            }

            if (s_egid == PrivilegedId)
            {
                // Don't cache when the user is privileged to change the id.
                return SysGetEGid();
            }

            return s_egid;
        }

        internal static uint GetEUid()
        {
            if (s_euid == InvalidId)
            {
                // When we can't determine the user is priviledged to change the id,
                // assume he is to avoid caching.
                if (!procfs.TryGetPermittedCapabilities(out procfs.Capabilities capabilities) ||
                    ((capabilities & procfs.Capabilities.CAP_SETUID) != 0))
                {
                    s_euid = PrivilegedId;
                }
                else
                {
                    s_euid = SysGetEUid();
                }
            }

            if (s_euid == PrivilegedId)
            {
                // Don't cache when the user is privileged to change the id.
                return SysGetEUid();
            }

            return s_euid;
        }

        internal static bool IsMemberOfGroup(uint gid)
        {
            if (gid == GetEGid())
            {
                return true;
            }

            uint egid = s_egid;
            Debug.Assert(egid != InvalidId); // We've called GetEGid.
            uint[]? groups;
            if (egid == PrivilegedId || egid == PrivilegedId)
            {
                groups = GetGroups();
                if (groups == null)
                {
                    return false;
                }

                return Array.IndexOf(groups, gid) >= 0;
            }

            groups = s_groups;
            if (groups == null)
            {
                groups = GetGroups();
                if (groups == null)
                {
                    return false;
                }

                Array.Sort(groups);

                s_groups = groups;
            }

            return Array.BinarySearch(groups, gid) >= 0;
        }
    }
}
