from aalogbeat import BaseTest

import os


class Test(BaseTest):

    def test_base(self):
        """
        Basic test with exiting Aalogbeat normally
        """
        self.render_config_template(
            path=os.path.abspath(self.working_dir) + "/log/*"
        )

        aalogbeat_proc = self.start_beat()
        self.wait_until(lambda: self.log_contains("aalogbeat is running"))
        exit_code = aalogbeat_proc.kill_and_wait()
        assert exit_code == 0
