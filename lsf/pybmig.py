#!/usr/bin/env python
# encoding: utf-8
"""
untitled.py

Created by Nick Crawford on 2010-09-27.
Copyright (c) 2010

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses

The author may be contacted at ngcrawford@gmail.com
"""


import os
import sys
import shlex
import argparse
import subprocess
from pyparsing import *
from datetime import datetime


# ----------
# Setup Arguements
# ---------

def getargs():
    parser = argparse.ArgumentParser(description='Process some integers.')

    parser.add_argument('-t','-time', nargs=2, default= [0,0], dest='input_time',
                        help="""Number of days and hours after which 
                                jobs should be killed and restarted.""")
                    
    parser.add_argument('-r','-restart-string', dest='rstring', 
                        default="bsub -q short_serial -R 'select[mem>3000]'",
                        help="""Use to set bsub commands. The default is:
                                bsub -q short_serial -R 'select[mem>3000]'""")
                    
    parser.add_argument('-q','-queue', dest='queue',
                        default='short_serial',
                        help='LSF queue to which to resubmit jobs.')

    parser.add_argument('-k','-kill-restart', dest='kill_restart', 
                        action='store_true',
                        help="""Kills and restarts hung jobs.  Jobs must be RUNNING, 
                                but have run longer than their queue allows.""")
                        
    parser.add_argument('-v','-verbose', action='store_true', dest='verbose',
                        help="Turn on verbose mode.")

    args = parser.parse_args()
    return args


# -------------------------
# String Cleaning Functions
# -------------------------
def cleanCommmand(parse_string,loc, toks):
    command = toks[0].split('\n')
    text = ''
    for count, item in enumerate(command):
        if count >= 1:
            item = item[21:]
        text += item
    text = text.strip()
    return text

def cleanDateTime(parse_string,loc, toks):
    
    date_object = datetime.strptime(toks[0], '%a %b %d %H:%M:%S: ') # notice extra colon and whitespace
    date_object = date_object.replace(2010) # will need updating next year
    return date_object

# --------------
# Misc. functions
# ---------------

def print_bjobs(shlex_job_list):
    command_string = ''
    for command in shlex_job_list:
            command_string += command + ' '
    return command_string

def kill_restart_job(item, bsub_prefix, verbose):
    # kill job
    kill_command = "bkill -R %s" % item.job # do hard '-R' kill
    kill_command = shlex.split(kill_command)
    subprocess.Popen(kill_command)
    
    # start new job
    command = "\'%s\'" % item.command
    new_job = bsub_prefix + ' ' + command
    new_job_split = shlex.split(new_job)
    if verbose:
    	print 'Reexecuting %s' % item.job
    	print new_job
    	subprocess.Popen(new_job_split)
    else:
	    subprocess.Popen(new_job_split)
	
    return 1

# --------------------------
# define basic words/tokens
# --------------------------

def parser():
    # define value
    lessthan = Literal(">").suppress()
    greaterthan = Literal("<").suppress()
    value = greaterthan + SkipTo(lessthan) + lessthan
    
    # define line
    line = LineStart() + SkipTo(Literal(';')) + Literal(';')
    
    # define date
    date_end = Literal('Started on').suppress()
    date =  line.suppress() + LineStart() + SkipTo(date_end) + date_end
    
    # named parts
    job = Keyword('Job') + value.copy().setParseAction(cleanCommmand).setResultsName('job') 
    user = Keyword('User') + value.copy().setParseAction(cleanCommmand).setResultsName('user')
    project = Keyword('Project') + value.copy().setParseAction(cleanCommmand).setResultsName('project')
    status = Keyword('Status') + value.copy().setParseAction(cleanCommmand).setResultsName('status')
    queue = Keyword('Queue') + value.copy().setParseAction(cleanCommmand).setResultsName('queue')
    command = Keyword('Command') + value.copy().setParseAction(cleanCommmand).setResultsName('command') # copy is key here
    share_group = Literal('Share') + SkipTo(value) + value.copy().setParseAction(cleanCommmand).setResultsName('share_group')
    date = date.setResultsName('date').setParseAction(cleanDateTime)
    
    tokens = job + SkipTo(user) \
            + user + SkipTo(project) \
            + project + SkipTo(status) \
            + status + SkipTo(queue) \
            + queue + SkipTo(command) \
            + command + SkipTo(share_group) \
            + share_group + SkipTo(date) \
            + date
            
    return tokens

tokens = parser()
args = getargs()


if not sys.stdin.isatty(): # checks that stdin is present.
    data = sys.stdin.read()
else:
    fin = open('bjobs.out','r')
    data = fin.read()

# do stuff with results
for item in tokens.searchString(data):
	# do time calculations
    time_diff = (datetime.today() - item.date)
    hours_elapsed = time_diff.days*24.0 + (time_diff.seconds/60.0)/60.0

    if args.input_time:
        args.input_time = [int(x) for x in args.input_time]
        min_time = args.input_time[0]*24 + args.input_time[1]
    if args.verbose:
        print "Queue:", item.queue
        print "Status:", item.status
        print "Hours Elapsed:", hours_elapsed
        print "Command:", item.command
        print "---------------------------------\n"

    # Auto determine which jobs to restart
    if args.kill_restart:
        if item.queue == 'short_serial':
            if item.status in ('RUN', 'SSUSP'):
                if hours_elapsed > min_time:
                    kill_restart_job(item, args.rstring, args.verbose)
                elif hours_elapsed > 1:
                    kill_restart_job(item, args.rstring, args.verbose)

        if item.queue == 'normal_serial':
            if item.status in ('RUN', 'SSUSP'):
                if hours_elapsed > min_time:
                    kill_restart_job(item, args.rstring, args.verbose)
                if hours_elapsed > 24:
                    kill_restart_job(item, args.rstring, args.verbose)
print 'restart completed'
sys.exit()
