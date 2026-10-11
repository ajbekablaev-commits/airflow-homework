# fcntl - заглушка для Windows
def fcntl(fd, op, arg=0): return 0
def ioctl(fd, op, arg=0, mutable_flag=True): return 0
def flock(fd, op): return 0
def lockf(fd, op, length=0, start=0, whence=0): return 0
F_GETFL = 0
F_SETFL = 0
LOCK_SH = 0
LOCK_EX = 0
LOCK_NB = 0
LOCK_UN = 0
