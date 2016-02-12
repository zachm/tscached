class MockRedis():
    get_call_count = 0
    set_call_count = 0
    get_parms = []
    set_parms = []
    success_flag = True

    def get(self, key):
        self.get_parms.append([key])
        self.get_call_count += 1
        return '{"hello": "goodbye"}'

    def set(self, key, value, **kwargs):
        self.set_parms.append([key, value, kwargs])
        self.set_call_count += 1
        return self.success_flag
