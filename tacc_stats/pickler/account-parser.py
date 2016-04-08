#!/usr/bin/env python
#
# @Copyright@
#
#                               Rocks(r)
#                        www.rocksclusters.org
#                        version 5.6 (Emerald Boa)
#                        version 6.1 (Emerald Boa)
#
# Copyright (c) 2000 - 2013 The Regents of the University of California.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
# 1. Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
# notice unmodified and in its entirety, this list of conditions and the
# following disclaimer in the documentation and/or other materials provided
# with the distribution.
#
# 3. All advertising and press materials, printed or electronic, mentioning
# features or use of this software must display the following acknowledgement:
#
#       "This product includes software developed by the Rocks(r)
#       Cluster Group at the San Diego Supercomputer Center at the
#       University of California, San Diego and its contributors."
#
# 4. Except as permitted for the purposes of acknowledgment in paragraph 3,
# neither the name or logo of this software nor the names of its
# authors may be used to endorse or promote products derived from this
# software without specific prior written permission.  The name of the
# software includes the following terms, and any derivatives thereof:
# "Rocks", "Rocks Clusters", and "Avalanche Installer".  For licensing of
# the associated name, interested parties should contact Technology
# Transfer & Intellectual Property Services, University of California,
# San Diego, 9500 Gilman Drive, Mail Code 0910, La Jolla, CA 92093-0910,
# Ph: (858) 534-5815, FAX: (858) 534-7345, E-MAIL:invent@ucsd.edu
#
# THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS''
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS
# BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
# BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN
# IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# @Copyright@
#

import subprocess
import hostlist
import time
import os, sys
import re

accounting_dir = "/opt/xsede_stats/comet_accounting"
hostfile_dir = "/opt/xsede_stats/comet_hostfile_logs"
archive_dir = "/opt/xsede_stats/comet_archive"

acct_out = subprocess.Popen(['ssh', '-i', '/opt/rocks/etc/id_rsa_xsede_stats_comet_ln3', 'dmishin@comet-ln3.sdsc.edu'], stdout=subprocess.PIPE)

if not os.path.exists(accounting_dir):
    os.makedirs(accounting_dir)

def dateToEpoch(date):
    return str(int(time.mktime(time.strptime(date, "%Y-%m-%dT%H:%M:%S"))))

def durationToSeconds(duration_str):
    h,m,s = re.split(':',duration_str)
    d = ''
    if('-' in h):
        d,h = re.split('-', h)
    result = int(h)*3600+int(m)*60+int(s)
    if(d): result = result + int(d)*24*60*60
    return result

with open('%s/xsede_jobs_completed'%accounting_dir, 'w') as acct:
    for line in acct_out.stdout:
        acct_elems = line.split("|")
        if(not re.search(r'^\d+$', acct_elems[0])):
            continue
        job_date = acct_elems[3]
        acct_elems[3] = dateToEpoch(acct_elems[3]) 
        acct_elems[4] = dateToEpoch(acct_elems[4]) 
        acct_elems[5] = dateToEpoch(acct_elems[5])
        acct_elems[7] = str(durationToSeconds(acct_elems[7]))
        acct_elems.insert(3, '')
        acct_elems.insert(10, '')
        nodes_list = [elem.rstrip() for elem in hostlist.expand_hostlist(acct_elems.pop())]
        acct.write(":".join(acct_elems)+"\n")
 
        hostlist_dir = "%s/%s/"%(hostfile_dir, job_date[:10].replace("-","/"))
        if not os.path.exists(hostlist_dir):
            os.makedirs(hostlist_dir)

        with open(hostlist_dir+"hostlist.%s"%(acct_elems[0]), "w") as hostlist_file:
            hostlist_file.write("\n".join(nodes_list))
