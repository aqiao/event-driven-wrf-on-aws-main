import os
import boto3

class Template2Script:
    def __init__(self, job_template, job_id, bucket, s3_client, zone=None):

        self.template = job_template
        self.job_id = str(job_id)
        self.monitor_home = "/fsx/monitor"
        self.bucket = bucket
        # Since this class run in lambda context, the only directory in lambda context can access is /tmp
        # and there is storage limitation is 512MB by default, so please don't place large file in it
        self.local_dir = "/tmp"

        self.job_options_map = {
            "name": "--job-name",
            "nodes": "--nodes",
            "cpus_per_task": "--gpus-per-task",
            "tasks_per_node": "--ntasks-per-node",
            "current_working_directory": "--chdir",
            "requeue": "--requeue",
            "dependency": "--dependency"
        }

        self.generator_func_map = {
            "job": "job_options_generator",
            "script": "script_generator"
        }

        self.prefix = "{}".format(job_template["job"]["name"])
        self.s3_client = s3_client

    def job_options_generator(self, file_pointer, template_dict):
        """
        :param file_pointer: point to sbatch script in /tmp folder
        :param template_dict: slurm job details in JSON format
        :return:
        """
        job_options = template_dict['job']
        for opt_key in job_options:
            if opt_key == "environment":
                env = job_options[opt_key]
                for env_key in env:
                    file_pointer.write("#SBATCH --export={}={}\n".format(env_key, env[env_key]))
            elif opt_key == "current_working_directory":
                # When retrying, sbatch script will be placed in /fsx/monitor
                # so replace current_working_directory with that path hardcode
                # or sbatch command can't find these scripts
                file_pointer.write("#SBATCH {}={}\n".format(self.job_options_map[opt_key], self.monitor_home))
            else:
                file_pointer.write("#SBATCH {}={}\n".format(self.job_options_map[opt_key], str(job_options[opt_key])))

    def script_generator(self, file_pointer, template_dict):
        script_content = template_dict['script']
        job_script_path = r'./{}_script.sh'.format(self.prefix)
        s3_obj_name = "monitor/{}_script.sh".format(self.prefix)
        with open(job_script_path, 'w') as script_pointer:
            script_pointer.write(script_content)
        self.s3_client.upload_file(job_script_path, self.bucket, s3_obj_name)
        file_pointer.write("source {}\n".format(job_script_path))

    def generate(self):
        job_sbatch_path = r'{}/{}.sh'.format(self.local_dir, self.prefix)
        s3_obj_name = "monitor/{}.sh".format(self.prefix)
        with open(job_sbatch_path, 'w') as file_pointer:
            file_pointer.write("#!/bin/bash\n")
            for key in self.template:
                func_name = self.generator_func_map[key]
                func = getattr(Template2Script, func_name)
                func(self, file_pointer, self.template)
        self.s3_client.upload_file(job_sbatch_path, self.bucket, s3_obj_name)


if __name__ == '__main__':
    # export AWS_ACCESS_KEY_ID=AKIAQOYARXLVYSWLBDEZ
    # export AWS_SECRET_ACCESS_KEY=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
    # export AWS_DEFAULT_REGION=us-east-2
    template = {
        "job": {
            "name": "wrf_domain_1",
            "nodes": 1,
            "cpus_per_task": 4,
            "tasks_per_node": 24,
            "current_working_directory": "/home/ec2-user",
            "dependency": "afterok:1:2",
            "requeue": "false",
            "environment": {
                "PATH": "/bin:/usr/bin/:/usr/local/bin/",
                "LD_LIBRARY_PATH": "/lib/:/lib64/:/usr/local/lib"
            }
        },
        "script": "#!/bin/bash\nsleep 10m\necho `hostname`"
    }
    session = boto3.session.Session()
    s3 = session.client("s3")
    zone = "domain_1"
    bucket = "test-event-driven-weather-forecast"
    generator = Template2Script(template, 1, bucket, s3, zone)
    generator.generate()
