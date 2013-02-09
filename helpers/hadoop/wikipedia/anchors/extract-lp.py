import sys

def all_anchors(anchors):
    with open(anchors, 'r') as inputfile:
        for line in inputfile:
            anchorname = line.rstrip('\n')[len("anchor:"):]
            yield anchorname

ianchor = all_anchors(sys.argv[1])
current = ianchor.next()

for line in sys.stdin:
    anchor, lp = line.rstrip('\n').rsplit('\t', 1)
    lp = int(lp)
    finished = False

    try:
        while not finished:
            ret = cmp(current, anchor)

            if ret < 0:
                current = ianchor.next()
                continue
            elif ret == 0:
                print "%s\t%s" % (anchor, lp)
                current = ianchor.next()
                continue

            finished = True
    except StopIteration:
        break

