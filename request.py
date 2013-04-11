#!/usr/bin/env python

import sys
sys.path.append('/R/avro/pylib/1.7.4')

import avro.protocol
import avro.ipc

def Main(args):
  with open('service.avsc') as f:
    service_avsc = f.read()

  ServiceProtocol = avro.protocol.parse(service_avsc)
  ListRequest = ServiceProtocol.types_dict['ListRequest']
  ListReply = ServiceProtocol.types_dict['ListReply']

  transceiver = avro.ipc.HTTPTransceiver(host='localhost', port=8486)
  requestor = avro.ipc.Requestor(
      local_protocol=ServiceProtocol,
      transceiver=transceiver,
  )

  list_request = {
    'req': {
      'dir_path': args[1],
    },
  }

  list_reply = requestor.request('list', list_request)
  print('Listing the content of %s' % list_reply['dir_path'])
  for path in list_reply['paths']:
    print(path)


if __name__ == '__main__':
  Main(sys.argv)
