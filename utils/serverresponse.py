from utils.requestresponse import RequestResponse

class ServerResponse(RequestResponse):

    def __init__(self, payload=None):
        super().__init__(payload)