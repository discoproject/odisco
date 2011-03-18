#!/usr/bin/python
import os, sys, getopt, pwd, socket, zipfile
import json

def owner():
    return "%s@%s" % (pwd.getpwuid(os.getuid()).pw_name,
                      socket.gethostname())

from disco import json
from disco.job import JobPack

def usage():
    print "Usage: %s [options]"
    print " -n: name of the job (required)"
    print " -M: location of master"
    print " -m: run the map phase"
    print " -r: run the reduce phase"
    print " -W: worker binary (required)"
    print " -D: work directory in which worker will run"
    print " -O: key or key=value pair passed to worker"
    print "     usual worker options are:"
    print "      partitions=N (for map phase)"
    print "      merge_partitions (for reduce phase)"
    print "      sort"
    print "      save (results are saved to DDFS)"
    print " --sched: key=value pair for scheduler"
    print "     usual scheduler options are:"
    print "      force_local, force_remote, max_cores=N"

def errmsg(msg):
    print msg
    sys.exit(1)

def validate(jobname, jobdict, worker, workdir):
    if not jobname:
        errmsg("Job name must be specified [-n]")
    if not jobdict["map?"] and not jobdict["reduce?"]:
        errmsg("One of map (-m) or reduce (-r) must be specified")

    if not worker:
        errmsg("The worker (-W) must be specified")
    else:
        if not os.access(worker, os.X_OK):
            errmsg("The worker needs to be an executable file")
        worker = os.path.normpath(worker)
    if workdir:
        if not os.path.isdir(workdir):
            errmsg("The work directory needs to be an existing directory")
        workdir = os.path.normpath(workdir)
        workerfile = os.path.join(workdir, os.path.basename(worker))
        if not (os.path.isfile(workerfile) and os.path.samefile(worker, workerfile)):
            errmsg("The worker should be present in the work directory")
    return worker, workdir

def pack(jobname, jobdict, worker, workdir):
    import cStringIO
    zmem = cStringIO.StringIO()
    z = zipfile.ZipFile(zmem, "w")
    # FIXME: Strip off leading pathname elements from zipped paths
    if not workdir:
        z.write(worker)
    else:
        def walker(arg, dirname, names):
            z.write(dirname)
            for n in names:
                z.write(os.path.join(dirname, n))
        os.path.walk(workdir, walker, None)
    z.close()

    def contents(*t):
        offset = JobPack.HEADER_SIZE
        for segment in t:
            yield offset, segment
            offset += len(segment)

    offsets, fields = zip(*contents(json.dumps(jobdict),
                                    json.dumps({}),
                                    zmem.getvalue(),
                                    ''))
    hdr = JobPack.header(offsets)
    return hdr + ''.join(fields)

def submit(master, jobpack):
    from disco.settings import DiscoSettings
    from disco.core import Disco
    settings = DiscoSettings()
    dmaster = Disco(master)
    print "Submitting job to ", master
    status, response = json.loads(dmaster.request('/disco/job/new', jobpack))
    if status != 'ok':
        errmsg('Failed to start job. Server replied: %s' % response)
    print response

def main():
    jobname, worker, workdir = None, None, None
    master = 'disco://localhost'
    jobdict = { "map?" : False,
                "reduce?" : False,
                "nr_reduces" : 1,
                "owner" : owner(),
                "worker_options" : [],
                "scheduler" : {}}
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hn:M:mrW:D:O:",
                                   ['help', 'num_reduces=','sched='])
        for o, a in opts:
            if o == '-h':
                usage()
            elif o == '-n':
                jobname = a
            elif o == '-M':
                master = a
            elif o == '-m':
                jobdict["map?"] = True
            elif o == '-r':
                jobdict["reduce?"] = True
            elif o == '-W':
                worker = a
            elif o == '-D':
                workdir = a
            elif o == '-O':
                jobdict["worker_options"].append(a)
            elif o == '--num_reduces':
                jobdict["nr_reduces"] = int(a)
            elif o == 'sched':
                kv = a.split('=')
                if len(kv) == 2:
                    jobdict["sched_options"][kv[0]] = kv[1]
                else:
                    jobdict["sched_options"][kv[0]] = None
    except getopt.GetoptError, err:
        print str(err)
        sys.exit(2)

    worker, workdir = validate(jobname, jobdict, worker, workdir)
    jobdict['prefix'] = jobname
    jobdict['worker'] = worker

    from disco.util import inputlist
    jobdict['input'] = args
    #jobdict['input'] = inputlist(args)
    #print inputlist(*args)

    jobpack = pack(jobname, jobdict, worker, workdir)

    submit(master, jobpack)

if __name__ == '__main__':
    main()
