from twitter.const import *

class TwitterResponse(object):
    def __init__(self, status, user_id, state, sleep_time=0):
        self.status = status
        self.user_id = user_id
        self.state = state
        self.sleep_time = sleep_time
        self.attributes = {}

    def __setitem__(self, key, value):
        self.attributes[key] = value

    def __getitem__(self, key):
        return self.attributes[key]

    def __contains__(self, key):
        return key in self.attributes

    def __str__(self):
        if self.status == STATUS_COMPLETED:
            status = 'COMPLETED'
        elif self.status == STATUS_BANNED:
            status = 'BANNED'
        elif self.status == STATUS_UNAUTHORIZED:
            status = 'UNAUTHORIZED'
        elif self.status == STATUS_ERROR:
            status = 'ERROR'
        else:
            raise Exception("Unknown status %s" % self.status)

        return ("%s user_id: %s state: %s sleep_time: %s attributes: %s" % (
            status,
            str(self.user_id),
            str(self.state),
            str(self.sleep_time),
            ', '.join(map(lambda x: str(x[0]) + "=" + str(x[1]), sorted(self.attributes.items())))
        )).strip()

    @classmethod
    def msg_to_status(cls, msg):
        return {
            MSG_OK:       STATUS_COMPLETED,
            MSG_BAN:      STATUS_BANNED,
            MSG_NOTFOUND: STATUS_UNAUTHORIZED,
            MSG_NOTAUTH:  STATUS_UNAUTHORIZED,
            MSG_UNK:      STATUS_ERROR
        }.get(msg, STATUS_ERROR)