import time
from datetime import datetime
from twitter import settings
from twisted.python.log import textFromEventDict

def _safeFormat(fmtString, fmtDict):
    try:
        text = fmtString % fmtDict
    except KeyboardInterrupt:
        raise
    except:
        try:
            text = ('Invalid format string or unformattable object in log message: %r, %s' % (fmtString, fmtDict))
        except:
            try:
                text = 'UNFORMATTABLE OBJECT WRITTEN TO LOG with fmt %r, MESSAGE LOST' % (fmtString,)
            except:
                text = 'PATHOLOGICAL ERROR IN BOTH FORMAT STRING AND MESSAGE DETAILS, MESSAGE LOST'
    return text

class RedisLogObserver:
    def __init__(self, redis):
        self.redis = redis
        self.timeFormat = None

    def getTimezoneOffset(self, when):
        offset = datetime.utcfromtimestamp(when) - datetime.fromtimestamp(when)
        return offset.days * (60 * 60 * 24) + offset.seconds

    def formatTime(self, when):
        if self.timeFormat is not None:
            return time.strftime(self.timeFormat, time.localtime(when))

        tzOffset = -self.getTimezoneOffset(when)
        when = datetime.utcfromtimestamp(when + tzOffset)
        tzHour = abs(int(tzOffset / 60 / 60))
        tzMin = abs(int(tzOffset / 60 % 60))
        if tzOffset < 0:
            tzSign = '-'
        else:
            tzSign = '+'
        # return '%d-%02d-%02d %02d:%02d:%02d%s%02d%02d' % (
        #     when.year, when.month, when.day,
        #     when.hour, when.minute, when.second,
        #     tzSign, tzHour, tzMin)
        return '%02d:%02d:%02d' % (when.hour, when.minute, when.second)

    def emit(self, eventDict):
        text = textFromEventDict(eventDict)
        if text is None:
            return

        timeStr = self.formatTime(eventDict['time'])
        fmtDict = {'system': eventDict['system'], 'text': text.replace("\n", "\n\t")}
        msgStr = _safeFormat("[%(system)s] %(text)s\n", fmtDict)

        self.redis.lpush(settings.LOG_LIST, timeStr + " " + msgStr)
        self.redis.ltrim(settings.LOG_LIST, 0, settings.LOG_SCROLLBACK)