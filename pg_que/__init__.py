

class PGQue:
    def __init__(self, connstring):
        self.message = None

    def get_queue(self, name):
        return self

    def send_message(self, message):
        self.message = message

    def receive_message(self):
        return self.message