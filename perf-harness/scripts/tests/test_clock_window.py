import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from benchmark import valid_clock_window

class ClockWindowTest(unittest.TestCase):
    def test_normal_docker_start_and_sampling_overhead_is_valid(self):
        self.assertTrue(valid_clock_window(190.1,190.1,180))
    def test_observed_host_sleep_is_not_a_three_minute_measurement(self):
        self.assertFalse(valid_clock_window(5970.61,190,180))
    def test_transient_clock_jump_cannot_be_hidden_by_a_later_correction(self):
        self.assertFalse(valid_clock_window(190,190,180,max_gap=2))
    def test_early_load_exit_is_not_a_complete_soak(self):
        self.assertFalse(valid_clock_window(60,60,7200))

if __name__=='__main__':unittest.main()
