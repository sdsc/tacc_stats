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
import time
import os, os.path
import re, sys, pwd
from rabbitmqclient import RabbitMQLocator
import pika
import json
from datetime import date, timedelta

class AccountParserPBS:
    def __init__(self):

        self._site = "sdsc.edu"
        self._keymap = { 'Exit_status' : 'status' }

        self.USERNAME = "xsede_stats"
        loc = RabbitMQLocator(self.USERNAME)
        self.RABBITMQ_PW = loc.RABBITMQ_PW
        self.RABBITMQ_URL = loc.RABBITMQ_URL
        self.ret_message = None


    def dateToEpoch(self, date):
        return str(int(time.mktime(time.strptime(date, "%Y-%m-%dT%H:%M:%S"))))

    def today(self):
        return time.strftime("%Y%m%d")

    def yesterday(self):
        yesterday = date.today() - timedelta(1)
        return yesterday.strftime('%Y%m%d')

    def durationToSeconds(self, duration_str):
        if(not duration_str):
            return 0
        h,m,s = re.split(':',duration_str)
        d = ''
        if('-' in h):
            d,h = re.split('-', h)
        result = int(h)*3600+int(m)*60+int(s)
        if(d): result = result + int(d)*24*60*60
        return result

    def _resourceListNodes(self, key, value):
        """Process NERSC 'Resource_List.nodes' field.
        """
        if value.find(":ppn=") > -1:
            nodes, ppnstring = value.split(':',1)
            ppnkey,ppnvalue = ppnstring.split('=',1)
            ppnvalue = ppnvalue.split(":")[0]
            if(not nodes.isdigit()):
        	nodes=1
            return (("nodes", nodes), (ppnkey, ppnvalue), ("num_procs", int(nodes)*int(ppnvalue)))
        else:
            return [[key, value]]

    def process(self, line):
        """Process one PBS job record.
        """
        # split out header fields
        timestamp, rectype, jobid, record = line.split(';',3)
        # convert the timestamp
        parsed_ts = time.strptime(timestamp, "%m/%d/%Y %H:%M:%S")
        d = dict(ts=time.mktime(parsed_ts), event="pbs.job." + rectype, type=rectype, job__id=jobid, site=self._site)
        # parse the record field's name=value pairs
        if record:
            for item in record.split(' '):
                k, v = item.split('=',1)
                if k == "other" and _other:
                    # call special function for 'other'
                    for kk, vv in _other(k,v):
                        d[kk] = vv
                elif k == "Resource_List.nodes":
                    for kk, vv in self._resourceListNodes(k,v):
                        d[kk] = vv
                else:
                    k = self._keymap.get(k, k)
                    d[k] = v
        return d

    def run(self):
        account_file = '/var/spool/torque/server_priv/accounting/%s'%self.today()
        if (not os.path.isfile(account_file)) or (time.strftime("%H%m") == "0005"):
            account_file = '/var/spool/torque/server_priv/accounting/%s'%self.yesterday()
        with open(account_file, 'rb') as acct_pbs:
            acct_list = []
            for line in acct_pbs:
                if re.search(';E;', line): # job is finished
                    acct_struct = self.process(line.rstrip())
                    if acct_struct.get("queue")!="shared":
                        jobid = acct_struct.get("job__id").split(".")[0]
                        
                        acct_list.append({"acct" : ":".join([ \
                            jobid, \
                            str(pwd.getpwnam(acct_struct.get("user")).pw_uid), \
                            str(acct_struct.get("group"))+":", \
                            str(acct_struct.get("start")), \
                            str(acct_struct.get("end")), \
                            str(acct_struct.get("qtime")), \
                            str(acct_struct.get("queue")), \
                            str(self.durationToSeconds(acct_struct.get("Resource_List.walltime"))), \
                            str(acct_struct.get("jobname"))+":", \
                            str(acct_struct.get("status")), \
                            str(acct_struct.get("nodes")) if acct_struct.get("nodes") else "1", \
                            str(acct_struct.get("num_procs")) if acct_struct.get("num_procs") else "1" \
                            ]), \
                            "nodes_list" : acct_struct.get("exec_host").split("+"), \
                            "time": acct_struct.get("ts"), \
                            "start_time": acct_struct.get("start"), \
                            "jobid": jobid
                            })

            self.send_msg(acct_list)    

    def send_msg(self, msg):
        credentials = pika.PlainCredentials(self.USERNAME, self.RABBITMQ_PW)
        parameters = pika.ConnectionParameters(self.RABBITMQ_URL,
                                                       5672,
                                                       self.USERNAME,
                                                       credentials)
        connection = \
            pika.BlockingConnection(parameters)

        channel = connection.channel()

        try:
            channel.basic_publish(exchange='stats-listener',
                    routing_key="",
                    body=json.dumps(msg, ensure_ascii=True),
                    properties=pika.BasicProperties(content_type='application/json', type="acct"))
        finally:
            connection.close()

if __name__ == "__main__":
   AccountParserPBS().run() 
