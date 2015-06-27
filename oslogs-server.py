#!/usr/bin/env python
import os
import re
import json
import traceback

import pika


try:
    fin = open('oslogs-server.conf', 'r')
    LOGS_FOLDER = re.search('LOGS_FOLDER=(.*)', fin.read()).group(1).strip()
    print 'LOGS_FOLDER found in config: "%s"' % LOGS_FOLDER
except Exception, err:
    print 'LOGS_FOLDER not found in config. Using "/var/log/oslogs"'
    LOGS_FOLDER = '/var/log/oslogs'
HOSTS = {}


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='0.0.0.0'))
    channel = connection.channel()
    channel.queue_declare(queue='oslogs')

    print '[*] Connected. Waiting for messages...'

    def callback(ch, method, properties, body):
        try:
            item = json.loads(body)
        except Exception, err:
            print 'Failed to load "%s"; error: "%s"' % body, err
            return
        item_received(item)

    channel.basic_consume(callback, queue='oslogs', no_ack=True)
    channel.start_consuming()


def item_received(item):
    try:
        host, path, msg = item['host'], item['path'], item['msg']
    except KeyError, err:
        print 'Failed to unpack "%s"; error: "%s"' % (item, err)
        return

    log_name = path.split('/')[-1]

    try:
        log = HOSTS[host][log_name]
    except KeyError:
        folder_path = os.path.join(LOGS_FOLDER, host)
        if not os.path.isdir(folder_path):
            try:
                os.mkdir(folder_path)
            except OSError, err:
                print 'Failed to create folder "%s"; error: "%s"' % (
                    folder_path, err
                )
                return
        if host not in HOSTS:
            HOSTS[host] = dict()
        log_path = os.path.join(folder_path, log_name)
        log_info_path = log_path + '.path'
        try:
            fout = open(log_info_path, 'w')
            fout.write(path)
            fout.close()
        except Exception, err:
            print 'Failed to write info for log "%s"; error: "%s"' % (log_path, err)
        try:
            HOSTS[host][log_name] = open(log_path, 'a')
        except Exception, err:
            print 'Failed to create log "%s"; error "%s"' % (log_path, err)
            return
        log = HOSTS[host][log_name]
    log.write(msg)
    log.flush()


if __name__ == '__main__':
    try:
        main()
    except Exception, err:
        traceback.format_exc(err)
