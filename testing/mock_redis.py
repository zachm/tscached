class MockRedis():

    def __init__(self):
        self.get_call_count = 0
        self.set_call_count = 0
        self.get_parms = []
        self.set_parms = []
        self.success_flag = True
        self.derived_pipeline = None

    def get(self, key):
        self.get_parms.append([key])
        self.get_call_count += 1
        return '{"hello": "goodbye"}'

    def set(self, key, value, **kwargs):
        self.set_parms.append([key, value, kwargs])
        self.set_call_count += 1
        return self.success_flag

    def pipeline(self):
        self.derived_pipeline = MockRedisPipeline()
        return self.derived_pipeline


class MockRedisPipeline():

    def __init__(self):
        self.execute_count = 0
        self.pipe_get_call_count = 0
        self.pipe_set_call_count = 0
        self.pipe_get_parms = []

    def execute(self):
        self.execute_count += 1
        return ['{"hello": "goodbye"}' for x in
                xrange(self.pipe_get_call_count + self.pipe_set_call_count)]

    def get(self, key):
        self.pipe_get_parms.append([key])
        self.pipe_get_call_count += 1
