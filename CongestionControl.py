SLOW_START=0
CONGESTION_AVOIDANCE=1

class CongestionControl:
    def __init__(self, MSS:int):
        self.MSS = MSS
        self.current_state = SLOW_START
        self.cwnd = MSS
        self.ssthresh=None
    def get_cwnd(self) -> int:
        return self.cwnd
    def get_MSS_in_cwnd(self) -> int:
        return int(self.get_cwnd() // self.MSS)
    def event_ack_received(self):
        if self.current_state == SLOW_START:
            self.cwnd += self.MSS
            if self.ssthresh and self.get_cwnd() >= self.ssthresh:
                self.current_state = CONGESTION_AVOIDANCE
        elif self.current_state == CONGESTION_AVOIDANCE:
            self.cwnd += 1/self.get_MSS_in_cwnd() * self.MSS
    def event_timeout(self):
        self.ssthresh = int(self.get_cwnd()/2)
        self.cwnd = self.MSS
        if self.current_state == CONGESTION_AVOIDANCE:
            self.current_state = SLOW_START
    def is_state_slow_start(self): return self.current_state == SLOW_START
    def is_state_congestion_avoidance(self): return self.current_state == CONGESTION_AVOIDANCE
    def get_ssthresh(self): return self.ssthresh 