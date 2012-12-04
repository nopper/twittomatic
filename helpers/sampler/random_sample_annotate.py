import sys
import gzip
from json import loads
from annotation import AnnotationExtractor

def main(inputfile, outputfile):
    with gzip.open(inputfile, 'r') as input:
        extractor = AnnotationExtractor()
        with open(outputfile, 'w') as output:
            for line in input:
                text = loads(line)['text']
                #annotations = extractor.annotate(line)
                annotations = extractor.annotate_simple(text)

                output.write("Text: %s\n" % text.encode('utf-8'))
                output.write("Annotations: %s\n" % str(annotations))

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
