# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import pandas


__author__ = "Jan Janssen"
__copyright__ = (
    "Copyright 2019, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "development"
__date__ = "Feb 9, 2019"


class LsfCommands(object):
    @property
    def submit_job_command(self):
        return ["bsub"]

    @property
    def stdin_as_input(self):
        return True

    @property
    def delete_job_command(self):
        return ["bkill"]

    @property
    def enable_reservation_command(self):
        raise NotImplementedError()

    @property
    def get_queue_status_command(self):
        return ["bjobs", "-noheader", "-o", "id user stat name delimiter=\"|\""]

    @staticmethod
    def get_job_id_from_output(queue_submit_output):
        return int(queue_submit_output.split()[1][1:-1])

    @staticmethod
    def convert_queue_status(queue_status_output):
        line_split_lst = [line.split('|') for line in queue_status_output.splitlines()]
        if len(line_split_lst) != 0 and line_split_lst[0][0] != 'No unfinished job found':
            job_id_lst, user_lst, status_lst, job_name_lst = zip(
                *[
                    (int(jobid), user, status.lower(), jobname)
                    for jobid, user, status, jobname in line_split_lst
                ]
            )
        else:
            job_id_lst, user_lst, status_lst, job_name_lst = [], [], [], []
        df = pandas.DataFrame(
            {
                "jobid": job_id_lst,
                "user": user_lst,
                "jobname": job_name_lst,
                "status": status_lst,
            }
        )
        df.loc[df.status == "run", "status"] = "running"
        df.loc[df.status == "pend", "status"] = "pending"
        return df
