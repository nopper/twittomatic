from collections import namedtuple

class Job(namedtuple('Job', 'operation, state, cursor')):
    @classmethod
    def deserialize(self, str):
    	if str is None:
            return None
        return Job(*str.split(',', 2))

    @classmethod
    def serialize(self, job):
        return '%s,%s,%s' % (job.operation, job.state, job.cursor)
