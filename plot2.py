import math
import os
import os.path
import re
import subprocess
import sys
import tempfile

def usage(msg=None):
    if msg is not None:
        print msg
    print "%s: directory filter_expr dimensions_to_plot" % sys.argv[0]
    print "Fields available for filtering and plotting are: "
    print ("op, file_sz, chunk_sz, buffer_sz, wall_time, cpu_use, bw, "
        "cpu_per_byte, crc, md5, weight, progress")
    sys.exit(1)

def read_file(f):
    res = []
    for line in open(f).readlines():
        if not line.lstrip():
            continue
        if line.lstrip().startswith('#'):
            continue
        (op, file_size, chunk_size, buffer_size, crc, md5, wall_time, cpu_time,
                prob, status, prog,
                version) = line.rstrip('\n').split(',')
        
        if status <> 'OK':
            print "Warning: not OK status"

        file_size = int(file_size)
        chunk_size = int(chunk_size)
        buffer_size = int(buffer_size)
        wall_time = int(wall_time)
        cpu_time = int(cpu_time)
        crc = crc == 'true'
        md5 = md5 == 'true'
        weight = 1/float(prob)
        prog = [[int(i) for i in time_point.split('|')] for time_point in
                prog.split(';')]

        sample = {}
        sample["op"] = op
        sample["file_sz"] = file_size / 1048576.
        sample["chunk_sz"] = chunk_size / 1048576.
        sample["buffer_sz"] = buffer_size / 1048576.
        sample["wall_time"] = wall_time
        sample["cpu_use"] = float(cpu_time) / wall_time
        sample["bw"] = float(file_size) / int(wall_time)
        sample["cpu_per_byte"] = float(cpu_time) / file_size
        sample["crc"] = crc
        sample["md5"] = md5
        sample["weight"] = weight
        sample["progress"] = prog

        res.append(sample)

    return res 

def escape(s):
    return s.replace('\\', '\\\\').replace('"', '\\"').replace('_', '\\_')

class Stats:
    def __init__(self):
        self.sum = 0.
        self.num = 0
        self.sum_square = 0.
        self.weight_sum = 0

    def add(self, x, weight):
        self.weight_sum = self.weight_sum + weight
        self.num = self.num + 1
        self.sum = self.sum + x * weight
        self.sum_square = self.sum_square + x*x * weight

    def show(self):
        ex = self.sum / self.weight_sum
        print "Samples=%s EX=%.02f, SD=%.02f" % (self.num, ex,
                math.sqrt(self.sum_square / self.weight_sum - ex * ex))

def run_gnuplot(res, fields, filt):
    f = tempfile.NamedTemporaryFile()
    print >>f, "$map1 << EOD"
    
    stats = Stats()
    for sample in res:
        weight = sample[-1]
        data = sample[:-1]
        stats.add(data[-1], weight)
        print >>f, ' '.join([str(s) for s in data])
    
    print >>f, "EOD"
    if len(fields) == 3:
        print >>f, "set title \"(%s, %s) -> %s for %s\"" % (fields[0],
                fields[1], fields[2], escape(filt))
        print >>f, "splot $map1 with points pt 7 palette"
    else:
        print >>f, "set title \"%s -> %s for %s\"" % (fields[0],
                fields[1], escape(filt))
        print >>f, "set yrange[0:%s]" % (max([sample[1] for sample in res]) *
                1.1)
        print >>f, "plot $map1 with points pt 7 "
    f.flush()
    stats.show()
    subprocess.Popen(['/usr/bin/gnuplot', f.name, '-']).wait()


def main():
    if len(sys.argv) <> 4:
        usage()
    file = sys.argv[1]
    filt = sys.argv[2]
    fields = sys.argv[3].split(',')
    if len(fields) <> 2 and len(fields) <> 3:
        usage("expected 2 or 3 comma-separated fields, got %s", len(fields))

    res = [[row[field] for field in fields] + [row["weight"]]
            for row in read_file(file) if eval(filt, None, row)]
    res.sort()
    run_gnuplot(res, fields, filt)


if __name__== "__main__":
    main()
