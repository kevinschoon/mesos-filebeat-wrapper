#!/usr/bin/env python
"""
filebeat-wrapper.py provides a simple wrapper for
use as a container logger with Mesos.
"""

import os
import sys
import json
import signal
import subprocess
import fileinput

template = """
filebeat:
  prospectors:
    -
      paths:
        - "-"
      input_type: stdin
      close_eof: true
      fields:
        image: {}
        framework_id: {}
        mesos_log_sandbox_directory: {}
        mesos_log_stream: {}

output:
  elasticsearch:
    hosts: ["{}"]
"""

template_json = """
{
  "mappings": {
    "_default_": {
      "_all": {
        "norms": false
      },
      "_meta": {
        "version": "6.0.0-alpha1"
      },
      "dynamic_templates": [
        {
          "fields": {
            "mapping": {
              "ignore_above": 1024,
              "type": "keyword"
            },
            "match_mapping_type": "string",
            "path_match": "fields.*"
          }
        }
      ],
      "properties": {
        "@timestamp": {
          "type": "date"
        },
        "beat": {
          "properties": {
            "hostname": {
              "ignore_above": 1024,
              "type": "keyword"
            },
            "name": {
              "ignore_above": 1024,
              "type": "keyword"
            }
          }
        },
        "input_type": {
          "ignore_above": 1024,
          "type": "keyword"
        },
        "message": {
          "norms": false,
          "type": "text"
        },
        "offset": {
          "type": "long"
        },
        "source": {
          "ignore_above": 1024,
          "type": "keyword"
        },
        "tags": {
          "ignore_above": 1024,
          "type": "keyword"
        },
        "type": {
          "ignore_above": 1024,
          "type": "keyword"
        }
      }
    }
  },
  "order": 0,
  "settings": {
    "index.refresh_interval": "5s"
  },
  "template": "filebeat-*"
}
"""

template_json_x2 = """
{
  "mappings": {
    "_default_": {
      "_all": {
        "norms": {
          "enabled": false
        }
      },
      "_meta": {
        "version": "6.0.0-alpha1"
      },
      "dynamic_templates": [
        {
          "fields": {
            "mapping": {
              "ignore_above": 1024,
              "index": "not_analyzed",
              "type": "string"
            },
            "match_mapping_type": "string",
            "path_match": "fields.*"
          }
        }
      ],
      "properties": {
        "@timestamp": {
          "type": "date"
        },
        "beat": {
          "properties": {
            "hostname": {
              "ignore_above": 1024,
              "index": "not_analyzed",
              "type": "string"
            },
            "name": {
              "ignore_above": 1024,
              "index": "not_analyzed",
              "type": "string"
            }
          }
        },
        "input_type": {
          "ignore_above": 1024,
          "index": "not_analyzed",
          "type": "string"
        },
        "message": {
          "index": "analyzed",
          "norms": {
            "enabled": false
          },
          "type": "string"
        },
        "offset": {
          "type": "long"
        },
        "source": {
          "ignore_above": 1024,
          "index": "not_analyzed",
          "type": "string"
        },
        "tags": {
          "ignore_above": 1024,
          "index": "not_analyzed",
          "type": "string"
        },
        "type": {
          "ignore_above": 1024,
          "index": "not_analyzed",
          "type": "string"
        }
      }
    }
  },
  "order": 0,
  "settings": {
    "index.refresh_interval": "5s"
  },
  "template": "filebeat-*"
}
"""

def main():
    # JSON encoded environment variable specified by the Mesos Executor
    MESOS_EXECUTOR_INFO = {}
    try:
        MESOS_EXECUTOR_INFO = json.loads(
            os.environ.get("MESOS_EXECUTORINFO_JSON"))
    except (ValueError, TypeError):
        pass
    # Path to the sandbox directory set by Mesos executor
    MESOS_LOG_SANDBOX_DIRECTORY = os.environ.get("MESOS_LOG_SANDBOX_DIRECTORY")
    # Name of the log stream (STDOUT or STDERR)
    MESOS_LOG_STREAM = os.environ.get("MESOS_LOG_STREAM")

    # When scheduling containers with Mesos the user can
    # optionally specify filebeat-template.json or
    # filebeat-template-es2x.json to override default
    # elasticsearch mappings. 
    filebeat_template_path = "{}/filebeat.template.json".format(MESOS_LOG_SANDBOX_DIRECTORY)
    if not os.path.isfile(filebeat_template_path) and MESOS_LOG_SANDBOX_DIRECTORY:
        with open(filebeat_template_path, "w") as fp:
            fp.write(template_json)
    filebeat_template_path_x2 = "{}/filebeat.template-es2x.json".format(MESOS_LOG_SANDBOX_DIRECTORY)
    if not os.path.isfile(filebeat_template_path_x2) and MESOS_LOG_SANDBOX_DIRECTORY:
        with open(filebeat_template_path_x2, "w") as fp:
            fp.write(template_json_x2)

    # Should be specified in the app configuration by the user.
    # If it is not specified do not attempt to log the output
    # but wait for EOF and then return zero.
    FILEBEAT_OUTPUT_HOST = ""
    try:
        FILEBEAT_OUTPUT_HOST = [
            x["value"] for x in MESOS_EXECUTOR_INFO["command"]["environment"]["variables"] 
            if x["name"] == "FILEBEAT_OUTPUT_HOST"
        ][0]
    except (KeyError, IndexError):
        # If FILEBEAT_OUTPUT_HOST is not specified
        # and a file with the corresponding stream name 
        # exists e.g. stdout then pipe the process stdin
        # to the corresponding file.
        if MESOS_LOG_SANDBOX_DIRECTORY and MESOS_LOG_STREAM:
            for line in fileinput.input("-"):
                with open("{}/{}".format(MESOS_LOG_SANDBOX_DIRECTORY, MESOS_LOG_STREAM.lower()), "a") as fp:
                    fp.write(line)
        # Otherwise redirect to stdout
        else:
            for line in fileinput.input("-"):
                sys.stdout.write(line)
        return None
    # Add Docker image and Framework ID fields for
    # Elasticsearch index.
    IMAGE = ""
    FRAMEWORK_ID = ""
    try:
        IMAGE = MESOS_EXECUTOR_INFO["container"]["docker"]["image"]
    except KeyError:
        pass
    try:
        FRAMEWORK_ID = MESOS_EXECUTOR_INFO["framework_id"]["value"]
    except KeyError:
        pass
    FILEBEAT_CONFIG_PATH = "{}/filebeat-{}.yml".format(
        MESOS_LOG_SANDBOX_DIRECTORY, MESOS_LOG_STREAM)
    # Write the filebeat yaml configuration to disk in the sandbox
    with open(FILEBEAT_CONFIG_PATH, "w") as fp:
        fp.write(
            template.format(
                IMAGE,
                FRAMEWORK_ID,
                MESOS_LOG_SANDBOX_DIRECTORY,
                MESOS_LOG_STREAM,
                FILEBEAT_OUTPUT_HOST))
    # Call filebeat, wait for it to exit, then return it's exit code.
    return subprocess.Popen(["/usr/bin/filebeat", "-path.config", MESOS_LOG_SANDBOX_DIRECTORY, "-c", FILEBEAT_CONFIG_PATH], stdin=sys.stdin)

if __name__ == "__main__":
    proc = main()
    if proc is None:
        sys.exit(0)
    def handler(signum, frame):
        proc.kill()
    signal.signal(signal.SIGTERM, handler)
    sys.exit(proc.wait())
