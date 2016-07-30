from PairedEndCommand import PairedEndCommand


class BWAMEM(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(BWAMEM, self).__init__(*args, **kwargs)

    def make_command(self, filename):
        raise RuntimeError("Not Yet Implemented")
        # TODO: Map with BWA mem
