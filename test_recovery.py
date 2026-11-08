import json
import multiprocessing
import os
from pathlib import Path
import sys
import tempfile
import unittest
ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT/'checkpoint-store'), str(ROOT/'collector')]
from eventweave_store import Journal, ConflictError
from eventweave_collector import collect, RetentionGap
CONFIG = {'endpoint':'https://example.invalid','start':10,'end':20,'filters':[]}
def event(i, ledger=12):
    return {'id': str(i), 'ledger': ledger, 'contractId':'fixture', 'topic':[], 'value':'AAAAAA=='}
def crash(path, job):
    journal = Journal(path)
    journal.commit_page(job, None, 'next', [event('crash')], fault=lambda:os._exit(73))

class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name)/'journal.sqlite')
        self.journal = Journal(self.path)
        self.job = self.journal.open_job(CONFIG)
    def tearDown(self):
        self.journal.close()
        self.tmp.cleanup()
    def test_atomic_exception_rollback(self):
        def fail():raise RuntimeError('injected crash')
        with self.assertRaises(RuntimeError):
            self.journal.commit_page(self.job,None,'a',[event(1)],fault=fail)
        self.assertEqual(self.journal.replay(self.job)['eventCount'],0)
        self.assertIsNone(self.journal.state(self.job)['cursor'])
    def test_process_death_rolls_back_wal(self):
        child=multiprocessing.get_context('spawn').Process(target=crash,args=(self.path,self.job))
        child.start();child.join(10)
        if child.is_alive():child.kill();child.join();self.fail('Crash child stalled')
        self.assertEqual(child.exitcode,73)
        self.journal.close();self.journal=Journal(self.path)
        self.assertEqual(self.journal.replay(self.job)['eventCount'],0)
        self.assertIsNone(self.journal.state(self.job)['cursor'])
    def test_duplicate_is_idempotent(self):
        self.journal.commit_page(self.job,None,'a',[event(1)])
        self.assertEqual(self.journal.commit_page(self.job,'a','b',[event(1)]),0)
        self.assertEqual(self.journal.replay(self.job)['eventCount'],1)
    def test_conflicting_duplicate_rolls_back_entire_page(self):
        self.journal.commit_page(self.job,None,'a',[event(1)])
        altered={**event(1),'value':'different'}
        with self.assertRaises(ConflictError):
            self.journal.commit_page(self.job,'a','b',[event(2),altered])
        self.assertEqual(self.journal.replay(self.job)['eventCount'],1)
        self.assertEqual(self.journal.state(self.job)['cursor'],'a')
    def test_stale_collector_cannot_advance(self):
        self.journal.commit_page(self.job,None,'a',[event(1)])
        with self.assertRaises(ConflictError):
            self.journal.commit_page(self.job,None,'b',[event(2)])
    def test_outside_range_rejected(self):
        with self.assertRaises(ValueError):self.journal.commit_page(self.job,None,'a',[event(1,20)])
    def test_order_independent_replay_hash(self):
        self.journal.commit_page(self.job,None,'a',[event(2,14),event(1,12)],True)
        other=Journal(str(Path(self.tmp.name)/'other.sqlite'))
        try:
            job=other.open_job(CONFIG);other.commit_page(job,None,'a',[event(1,12),event(2,14)],True)
            self.assertEqual(self.journal.replay(self.job)['replayHash'],other.replay(job)['replayHash'])
        finally:other.close()
    def test_filter_identity_isolated(self):
        self.assertNotEqual(self.job,self.journal.open_job({**CONFIG,'filters':[{'type':'contract'}]}))
    def test_incomplete_page_needs_new_cursor(self):
        with self.assertRaises(ConflictError):self.journal.commit_page(self.job,None,None,[event(1)])
        self.assertEqual(self.journal.replay(self.job)['eventCount'],0)
    def test_complete_job_immutable(self):
        self.journal.commit_page(self.job,None,None,[],True)
        with self.assertRaises(ConflictError):self.journal.commit_page(self.job,None,'a',[event(1)])
    def test_resume_deduplicates_and_omits_ledger_params(self):
        self.journal.commit_page(self.job,None,'a',[event(1)])
        calls=[]
        def request(url,method,params):
            calls.append((method,params))
            return {'oldestLedger':1,'latestLedger':30,**({'events':[event(1)],'cursor':'b'} if method=='getEvents' else {})}
        result=collect(self.journal,CONFIG,request,page_limit=2)
        params=calls[-1][1]
        self.assertNotIn('startLedger',params);self.assertNotIn('endLedger',params)
        self.assertEqual(params['pagination']['cursor'],'a')
        self.assertEqual(result['eventCount'],1);self.assertEqual(result['coverage'],'rpc-range-exhausted')
    def test_retention_gap_never_advances(self):
        def request(*args):return {'oldestLedger':11,'latestLedger':30}
        with self.assertRaises(RetentionGap):collect(self.journal,CONFIG,request)
        self.assertEqual(self.journal.state(self.job)['pages'],0)
    def test_future_range_rejected(self):
        with self.assertRaises(ValueError):collect(self.journal,CONFIG,lambda*a:{'oldestLedger':1,'latestLedger':18})
    def test_max_pages_remains_incomplete(self):
        def request(url,method,params):
            return {'oldestLedger':1,'latestLedger':30,**({'events':[event(1)],'cursor':'a'} if method=='getEvents' else {})}
        result=collect(self.journal,CONFIG,request,page_limit=1,max_pages=1)
        self.assertEqual(result['coverage'],'incomplete')
    def test_unordered_response_rejected(self):
        def request(url,method,params):
            return {'oldestLedger':1,'latestLedger':30,**({'events':[event(1,14),event(2,12)]} if method=='getEvents' else {})}
        with self.assertRaises(ValueError):collect(self.journal,CONFIG,request)
    def test_cursor_page_cannot_escape_fixed_range(self):
        self.journal.commit_page(self.job,None,'a',[event(1)])
        def request(url,method,params):
            return {'oldestLedger':1,'latestLedger':30,**({'events':[event(2,19),event(3,20)],'cursor':'b'} if method=='getEvents' else {})}
        result=collect(self.journal,CONFIG,request,page_limit=2)
        self.assertEqual(result['eventCount'],2)
        self.assertTrue(all(e['ledger']<20 for e in result['events']))

if __name__=='__main__':unittest.main()
