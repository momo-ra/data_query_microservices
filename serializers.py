class Serializer:
    def __init__(self, data):
        self.data = data

    def table_data_serializer(self):
        table_name = self.data["table_name"]
        
