class MorphIt(object):
    def __init__(self, morphfile="morph-it.txt"):
        self.file = open(morphfile, 'r')

        self.file.seek(0, 2)
        self.filesize = self.file.tell()
        self.file.seek(0, 0)

    def find(self, keyword):
        left, right = 0, self.filesize

        while left < right:
            position = (left + right) / 2
            self.file.seek(position, 0)

            # Read the next line since we could be in the middle of one
            _ = self.file.readline()
            position = self.file.tell()

            #print position, right, left
            if position == right or position == left:
                break

            line = self.file.readline().strip()
            word, root, wtype = line.split()

            if word == keyword:
                return word, root, wtype

            print "Line:", line, self.file.tell(), right

            if self.file.tell() == right:
                break

            if word > keyword.encode("utf8"):
                right = position
            else:
                left = position

if __name__ == "__main__":
    import sys
    print MorphIt().find(sys.argv[1])
