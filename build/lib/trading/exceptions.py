class Error(Exception):
    """Base class for exceptions in trading."""
    pass

class CouldNotQualifyContract(Error):
    """Exception raised if a contract could not be qualified.

    Attributes:
        contract -- the contract that could not be qualified
        message -- explanation of the error
    """

    def __init__(self, contract):
        self.contract = contract
        self.message = f'could not qualify contract {contract}'