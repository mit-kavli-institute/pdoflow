import enum


class PostingStatus(enum.Enum):
    paused = 1
    executing = 2
    finished = 3
    errored_out = 4


class JobStatus(enum.Enum):
    waiting = 1
    executing = 2
    done = 3
    errored_out = 4

    def exited(self) -> bool:
        cls = type(self)
        return self in (cls.done, cls.errored_out)
