#!/usr/bin/env python

import sys
import os
import errno
import time
import argparse
import subprocess as sp
import itertools as it

SAMPLE_NAMES = [
    "ZnunuB", "ZnunuC", "ZnunuL",
    "ZeeB", "ZeeC", "ZeeL",
    "ZmumuB", "ZmumuC", "ZmumuL", 
    "ZtautauB", "ZtautauC", "ZtautauL",
    "WenuB", "WenuC", "WenuL",
    "WmunuB", "WmunuC", "WmunuL",
    "WtaunuB", "WtaunuC", "WtaunuL", 
    "ttbar", "singletop_s", "singletop_t", "singletop_Wt",
    "WW", "WZ", "ZZ", 
    "monoWjj", "monoZjj",
#    "data",
    "data_extended",
]

DEFAULT_NFILE_PER_JOB = 5

NFILES_SPEC = {
    1: ['Znunu*', 'Wenu*'],
    2: ['Zee*', 'Zmumu*',],
}

def get_nfiles(sample_name):
    for n,specs in NFILES_SPEC.items():
        for s in specs:
            if s.endswith('*') and sample_name.startswith(s[:-1]):
                return n
            if sample_name == s:
                return n
    return DEFAULT_NFILE_PER_JOB

def split(iterable, n):
    acc = []
    for i,x in enumerate(iterable):
        acc.append(x)
        if i%n==(n-1):
            yield tuple(acc)
            acc = []
    # return the remainder, if any
    if len(acc):
        yield tuple(acc)

def xrd_files_recursive(host, base_path):
    with open(os.devnull, 'w') as devnull:
        xrd_listing = sp.check_output(['xrdfs', host, 'ls', '-l', base_path], stderr=devnull).split('\n')

    dirs = []
    files = []
    for l in xrd_listing:
        path = '/%s'%'/'.join(l.split('/')[1:])
        if l.startswith('d'):
            dirs.append(path)
        elif l.startswith('-'):
            files.append(path)

    sub_files = it.chain.from_iterable(map(lambda d: xrd_files_recursive(host, d), dirs))
    files.extend(sub_files)

    return set(files)

class LocalQueue:
    def __init__(self, nslots):
        self.nslots = nslots
        self.running_jobs = set()
        self.finished_jobs = []

    def check_jobs(self):
        finished = filter(lambda j: j.poll() is not None, self.running_jobs)
        map(self.running_jobs.remove, finished)
        self.finished_jobs.extend(finished)
        return len(finished)

    def has_slots(self):
        self.check_jobs()
        return len(self.running_jobs) < self.nslots

    def add_job(self, job):
        self.running_jobs.add(job)

    def wait(self, interval=2, verbose=False):
        self.check_jobs()
        if verbose:
            sys.stdout.write('\n')
        while len(self.running_jobs):
            if verbose:
                sys.stdout.write("\r%s\r"%(" "*80))
                sys.stdout.write("%d running\t%d finished" % (len(self.running_jobs), len(self.finished_jobs)))
                sys.stdout.flush()
            time.sleep(interval)
            self.check_jobs()

def submit_job(config_file, job_path, files, sample_name, local=False, slurmopts=None):
    out_path = os.path.join(job_path, 'outputs')

    list_path = os.path.join(job_path, 'file_lists')
    list_file = os.path.join(list_path, '%s.list'%sample_name)
    try:
        os.makedirs(list_path)
    except OSError as e:
        # if the path already exists, that's okay.
        if e.errno != errno.EEXIST: raise

    log_path = os.path.join(job_path, 'logs')
    log_file = os.path.join(log_path, '%s.log'%sample_name)
    try:
        os.makedirs(log_path)
    except OSError as e:
        # if the path already exists, that's okay.
        if e.errno != errno.EEXIST: raise

    with open(list_file, 'w') as flist:
        flist.write('\n'.join(files))
        flist.write('\n')

    cmd = [
        './BatchSubmit_gpatlas.sh',
        '-c', config_file,
        '-f', list_file,
        '-s', sample_name,
        '-o', out_path
        ]

    if not local:
        # run on batch
        sbatch_cmd = ['sbatch', '-t', '180',
                '-c', '1',
                '-p', 'atlas_all',
                '-o', log_file,
                '-J', sample_name
                ]
        if slurmopts:
            sbatch_cmd += slurmopts.split()
        sp.call(sbatch_cmd + cmd)

    else:
        # run locally
        f_log = open(log_file, 'w')
        err_file = os.path.join(log_path, '%s.err'%sample_name)
        f_err = open(err_file, 'w')
        job = sp.Popen(cmd, stdout=f_log, stderr=f_err)
        job._f_log = f_log
        job._f_err = f_err
        return job

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Submit jobs to the gpatlas SLURM cluster, using files from local XrootD.")
    parser.add_argument("--xrd-base", dest="xrd_base", default="/atlas/local/cshimmin/complete", help="The base XrootD path in which to look for data (i.e. the path containing the <DERIVATION>/<TAG>/ directories)")
    parser.add_argument("--xrd-host", dest="xrd_host", default="gpatlas2-ib.local", help="The main xrootd redirector server")
    parser.add_argument("--local", action="store_true", help="Run in local mode (rather than submit to batch)")
    parser.add_argument("--njobs", type=int, default=4, help="Number of concurrent jobs to run (local mode only).")
    parser.add_argument("--tag", default="00-16-01", help="The CxAOD data tag to use")
    parser.add_argument("--out", help="The path to place all generated file lists, logs, and outputs")
    parser.add_argument("--retrylist", help="A file containing the names of jobs (e.g. ttbar_11) you'd like to resubmit. Assumes the inputfile lists are already generated.")
    parser.add_argument("--slurmopts", help="Additional options to pass to slurm sbatch (make sure to put them in quotes!)")
    parser.add_argument("n_lep", type=int, help="The lepton region to use (0,1,2)")
    args = parser.parse_args()

    # set the derivation name
    derivations = {
            0: "HIGG5D1",
            1: "HIGG5D2",
            2: "HIGG2D4",
        }

    if not args.n_lep in derivations:
        print >> sys.stderr, "Invalid n_lep: %d" % args.n_lep
        sys.exit(1)
    selected_derivation = derivations[args.n_lep]

    if not args.out:
        args.out = "batch_%s_%s" % (selected_derivation, args.tag)
        print "No output path specified!"
        print "Will use:", args.out

    config_file = "data/FrameworkExe_monoVH/framework_monoVH-read_%s.cfg" % (selected_derivation)

    xrd_base_path = "{0}/{1}_13TeV/CxAOD_{2}".format(args.xrd_base, selected_derivation, args.tag)

    if args.retrylist:
        if args.local:
            print >>sys.stderr, "Local resubmit not supported!"
            sys.exit(1)
        print "Will resubmit file lists from", args.retrylist
        # read in input files, stripping whitespace and ignoring empty lines or lines starting with "#"
        input_lists = [l.strip() for l in open(args.retrylist)]
        input_lists = filter(lambda l: len(l) and not l.startswith('#'), input_lists)

        # get a list of sample names by stripping the first part of the SampleName_<N> pair
        sample_names = set([l.split('_')[0] for l in input_lists])

        for job_name in input_lists:
            list_file = open(os.path.join(args.out, 'file_lists', '%s.list'%job_name))
            input_files = [l.strip() for l in list_file]
            list_file.close()
            print "Resubmitting", job_name
            submit_job(config_file, args.out, input_files, job_name, local=False, slurmopts=args.slurmopts)
        print "All jobs resubmitted."
        sys.exit()

        # limit the list of samples used in the other loops to the ones found here.
        SAMPLE_NAMES = list(sample_names)

    else:
        print "Listing files from XrootD..."
        sample_files = {}
        for sample in SAMPLE_NAMES:
            xrd_path = "%s/%s" % (xrd_base_path, sample)
            files = xrd_files_recursive(args.xrd_host, xrd_path)
            # prepend the xrootd scheme:
            files = map(lambda f: "root://%s/%s"%(args.xrd_host, f), files)
            sample_files[sample] = files

        sample_jobs = {}
        total_jobs = 0
        for sample in SAMPLE_NAMES:
            nfile_per_job = get_nfiles(sample)
            job_lists = list(split(sample_files[sample], nfile_per_job))
            total_jobs += len(job_lists)
            sample_jobs[sample] = job_lists
        
    print "Submitting jobs..."
    queue = LocalQueue(args.njobs)
    tstart = time.time()
    for sample in SAMPLE_NAMES:
        job_lists = sample_jobs[sample]

        print
        print "Sample: %s\t| nfiles/job: %d\t| njob: %d" % ( sample, nfile_per_job, len(job_lists))
        print

        for i,j in enumerate(job_lists):
            while args.local and not queue.has_slots():
                n_finish = len(queue.finished_jobs)
                n_run = len(queue.running_jobs)
                n_queue = total_jobs-n_finish-n_run
                sys.stdout.write("\r%s\r"%(" "*80))
                sys.stdout.write("%d queued\t%d running\t%d finished\t(%ds elapsed)" % (n_queue, n_run, n_finish, int(time.time()-tstart)))
                sys.stdout.flush()
                time.sleep(2)
            sample_name = "%s_%d" % (sample, i)
            job = submit_job(config_file, args.out, j, sample_name, local=args.local, slurmopts=args.slurmopts)
            #print "Submitted job:", sample_name
            if args.local:
                queue.add_job(job)

    if args.local:
        print 
        print "Waiting for jobs to finish..."
        queue.wait(verbose=True)

        return_codes = [j.poll() for j in queue.finished_jobs]
        if any(return_codes):
            print "Warning! Had some nonzero returncodes!"
            print return_codes
        else:
            print "All jobs returned 0."
