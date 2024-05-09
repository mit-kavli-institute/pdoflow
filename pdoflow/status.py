import sqlalchemy as sa


class PostingStatus(sa.Enum):
    paused = 1
    executing = 2
    finished = 3
    errored_out = 4


class JobStatus(sa.Enum):
    waiting = 1
    executing = 2
    done = 3
    errored_out = 4
