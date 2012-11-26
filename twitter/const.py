MSG_OK = 0
MSG_BAN = 1
MSG_NOTAUTH = 2
MSG_LIMIT = 3
MSG_UNK = 4
MSG_NOTFOUND = 5
MSG_EXC = 6

MAX_ATTEMPTS = 4

BATCH_LIMIT = 100

STATUS_COMPLETED    = 0 # Implies nothing
STATUS_BANNED       = 1 # Implies waiting a sleep time
STATUS_UNAUTHORIZED = 2 # Implies removal of the file
STATUS_ERROR        = 3 # Implies notification of the error