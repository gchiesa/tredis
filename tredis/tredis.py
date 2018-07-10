#!/usr/bin/env python
import multiprocessing
import os
import random
from time import sleep

import redis
import rediscluster
from schwifty import IBAN

from .bics import DUTCH_BICS

__author__ = "Giuseppe Chiesa"
__copyright__ = "Copyright 2017, Giuseppe Chiesa"
__credits__ = ["Giuseppe Chiesa"]
__license__ = "BSD"
__maintainer__ = "Giuseppe Chiesa"
__email__ = "mail@giuseppechiesa.it"
__status__ = "PerpetualBeta"

DEFAULT_REDIS_HOST = os.environ.get('REDIS_HOST', None) or '127.0.0.1'
DEFAULT_REDIS_PORT = os.environ.get('REDIS_PORT', None) or '6379'
WORKERS = multiprocessing.cpu_count() * 2
# WORKERS = 1

DATA = u'3V53yB0Amdn764Ca8oUA5n7Ppc5kzkE1q0qByzwSth0a97IO7HE41sY56c4s26ht7udiZaiUXq4dIjofg_bo30VW3SGB7Y5i9JaIVzEMv14zwFVBFLDwQL2VcmPqusGSRuq4ULO1Ztbl4mCO_I7xPj67W3n0AMNYM831BGr560lqTtV2PTCayj9Abls2y977nA5NaJhBq4scNHBD1523XKB6C6_I7KnZ4JL9VcsU8EzgzO01303kxEcnSNS0tSrVu3g2Jl_4x626X6RUgX4hE0q9uzKVBmEl9C1V2O8FPf4i5Um3kPdKQQA9asZi6Tjd2aaUCdF1RJ43mMoUkz3PahXf9d3D_G0a759PV6Iwv6ln7ub5xg0HS98D7v0g9IFe7NO9525t7V4eA26X81M2AlnmUOjSPQJ4PrkmLqBNXkTIU_liEErae4__549V9rPMN8Gc09G63S05Pq8783z4O5TmBZ3UxyF6WqeZTD15oxqkjeZxeEBH95Q1CZXUgaIzPPIWCeuKDQoY666y9jtI6hj4HqJS8ysT74xEl6QY3jZ88n0Vmh7554ruJ4Ff3uDKx1x8MFqLr9kYtBC9i5p97DHb8aoiQYLgHSMEKCYboogw36xt0nuoOm4oQBPkr9R1rmh1hqsceGy6EVHQzq7k_p9lEOfV61pgoK_h__D33eEy6g6YaKO5cR21EenNp1I7Fyb32Sy05o64i0N0y0I77h618Re44oYUtv0Un9iNXhn8y1F3s238XM2M16hxmD14gzH26cSRgshIReEg46j3J3l9DG43TBFC67MX5YXzozkLReTf7d5qX1L55irl85IUp7XqPs8q6ieANN7ERVqMF9K497F6LcduJnQgO45Dyb5xZSrJrq51jrrA7ijVI48PPy98qN10USAXYRZlvT2KltP3sFvY7r3FSOKVKFEnzif_MzZ7jKaRg0izBrs0EERqKe9Y0Vb2UGk0ZBjp03q0h5QJBoCWefBdsa8o9nOCOn7Mo16K2Lhdk65rT5wxvBXL5FCTCdYo88VY99y0yrjU4k6dTJKf6Jes48nP416SYPykbSsalOp1W1Nc7KlODtAAtcvwN30ZJrLr9rXG790yKb200eyyCj0DpgxF2wEE0EB6N08Rm1ZJr41_P0JU3TcJ6iemuO83Saso83kx03ffFoiH70cMgBBtfZxpn6a71o27qaHW8DN2wcEePtyQ7gp705ufC0APq_RE54U5FEowUn9wJM0AjgldcmS5P5X9z0Nt9bcVTKc3E1JY7_pM5L_a83LT8Z3h9FNSd08oo80Opn8Zeh5p_5UyJ681Jk2J1QU3OsJ609f7zN7EUlAFM61fdgxo39trNgw4QCn6BhU_98gPYIrb70WA81BN9WHAPw6tsvbP4Q7hDN1T4fXnuzxP4415b1_m30mSxY99sqMo03c0e4Z4zU4WQ7NZaNkr85kn69w4796pijS87Iu153gU1X2uQRSH2oGgP3_4732XfMqJv8t48BA5Rlv4b4B1O2GP9e0x6OXmYZMGyvv1IJjcl3WsQ98yxbtD8g87dOWG9gNXd09UHx0EUse6Xq82FX3z7e820wjQ7r97R5Bj2ykwfW_p1U0sB8fKQ6tpLZ91tAGToWYp0o3cI4nIa7AkKrJhfdp323m5zzZ4TY30R9AEupe5I39Ns6mG7U0vJB0r8R1GJRj8VerJsiBVOZR9V1C3Y16s38tSK_Wf08vqtlshANzFbgp1KSOc920R1U1fcGXGnbmCKz76dPlzS4JGRLzz1tj7fI8zq2mhQ5ZWjaK0JfH9iW0eDGZd3Ly_9JgA46a06wDW0ICLgWQ94DnblBz_Hp8pi35VHynO53sc2ZpOMnP5S70Eeb2214ch4BlEuw928Ie92Op5P385U3KmNxBszoMH6bFS86RMyvFJ6Oh1C9Qv7He2o5v204V0U1E01nwoRwmhurXT2Y3eveCBQIUR5MF_hlFOQZ05PH45Hbv6JCRt_m69IHsvD6VyxArvb9jTv63yJ5R7jdgq62vPFal45XEiIJfeQt33t3Uz56Eo'


class Feeder(multiprocessing.Process):
    def __init__(self, task_queue, config, result):
        """

        :type task_queue: multiprocessing.JoinableQueue
        """
        super(Feeder, self).__init__()
        self.task_queue = task_queue
        self.config = config
        self.result = result
        self.result[self.name] = 0
        # self.client = redis.StrictRedis(host=config['host'], port=config['port'])
        startup_nodes = [{'host': self.config['host'], 'port': self.config['port']}]
        self.client = rediscluster.StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True,
                                                      readonly_mode=False, skip_full_coverage_check=True)

    def run(self):
        feed = 0
        while True:
            next_task = self.task_queue.get()
            if next_task is None:
                print('Exiting ${p}'.format(p=self.name))
                self.task_queue.task_done()
                self.result['generator_enabled'] = False
                break
            next_task(client=self.client)
            self.task_queue.task_done()
            feed += 1
            if feed > 5000:
                print('${p} updating records. My total: {t}'.format(p=self.name, t=(self.result[self.name]+feed)))
                self.result['total'] += feed
                self.result[self.name] += feed
                feed = 0


class IBANGenerator(multiprocessing.Process):
    CANARY = ('IBANGenerator-CANARY-KEY', 'IBANGenerator-CANARY-VALUE')

    def __init__(self, task_queue, result, country='NL', data_size=2048):
        super(IBANGenerator, self).__init__()
        self.task_queue = task_queue
        """ :type multiprocessing.JoinableQueue """
        self.result = result
        self.country = country
        self.data_size = data_size
        self.canary_queued = False
        self.result['generated'] = 0

    def run(self):
        generated = 0
        while self.result['generator_enabled']:
            key, value = self.generate_data()
            self.task_queue.put(RedisInsert(key, value))
            generated += 1
            if generated > 10000:
                print('{p} generated until now: {n} records'.format(p=self.name, n=(self.result['generated']+generated)))
                self.result['generated'] += generated
                generated = 0
        print('Exiting generator')

    def generate_data(self):
        if not self.canary_queued:
            self.canary_queued = True
            return self.CANARY
        account = random.randint(0, 9999999999)
        bank = random.choice(DUTCH_BICS)
        key = IBAN.generate(self.country, bank, str(account))
        # data = strgen.StringGenerator('[\d\w]{{{size}}}'.format(size=self.data_size)).render()
        data = DATA
        return key, data


class RedisInsert(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __call__(self, *args, **kwargs):
        client = kwargs.get('client', None)
        if client is None:
            raise ValueError('Connection required')
        client.set(self.key, self.value)


def main():
    if os.path.exists('/tmp/stopredis'):
        os.unlink('/tmp/stopredis')
    manager = multiprocessing.Manager()
    result = manager.dict()
    result['generator_enabled'] = True
    result['total'] = 0
    config = manager.dict()
    config['host'] = DEFAULT_REDIS_HOST
    config['port'] = DEFAULT_REDIS_PORT
    queue_data = multiprocessing.JoinableQueue()
    print('Configuration: {}'.format(config))
    print('Allocating feeders')
    feeders = [Feeder(queue_data, config, result) for _ in xrange(WORKERS)]
    for feeder in feeders:
        feeder.start()
    print('Allocating generator')
    generator = IBANGenerator(queue_data, result)
    generator.start()

    startup_nodes = [{'host': config['host'], 'port': config['port']}]
    client = rediscluster.StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True,
                                             readonly_mode=False, skip_full_coverage_check=True)

    while True:
        sleep(1)
        if os.path.exists('/tmp/stopredis'):
            break
        if result['total'] == 0:
            continue
        if client.get(IBANGenerator.CANARY[0]) is None:
            break
        if result['total'] % 10000 == 0:
            print('Generation in progress [{}]'.format(result['total']))
    # canary lost poisoning pill
    result['generator_enabled'] = False
    for _ in feeders:
        queue_data.put(None)
    print('WAITING FOR FEEDER PROCESSES')
    for f in feeders:
        f.terminate()
    queue_data.join()
    print('JOB DONE!')
    print('RESULTS\n{}'.format(result))


if __name__ == '__main__':
    main()
