import sqlalchemy as sa
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
