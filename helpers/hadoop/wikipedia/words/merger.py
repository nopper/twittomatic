import gzip
import heapq
import glob

def merge_files(inputfiles):
    files = []
    for filename in inputfiles:
        files.append(iter(gzip.open(filename, 'r')))

    iterator = iter(heapq.merge(*files))
    line = iterator.next()

    prevanchor, prevcounter = line.rstrip('\n').rsplit('\t', 1)
    prevcounter = int(prevcounter)

    for line in iterator:
        anchor, counter = line.rstrip('\n').rsplit('\t', 1)

        if anchor == prevanchor:
            prevcounter += int(counter)
        else:
            print "%s\t%s" % (prevanchor, prevcounter)
            prevanchor = anchor
            prevcounter = int(counter)

    print "%s\t%s" % (prevanchor, prevcounter)


files = glob.glob('*.tsv.gz')
merge_files(files)
