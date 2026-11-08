"""Read-only pilot using the system curl TLS trust store. No signing or funding."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
ROOT=Path(__file__).resolve().parent.parent
sys.path[:0]=[str(ROOT/'checkpoint-store'),str(ROOT/'collector')]
from eventweave_store import Journal
from eventweave_collector import collect

def request(url,method,params):
    result=subprocess.run(['curl','--fail','--silent','--show-error','--max-time','30',
        '--header','Content-Type: application/json','--data-binary',
        json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params}),url],
        check=True,capture_output=True,text=True)
    payload=json.loads(result.stdout)
    if 'error' in payload:raise RuntimeError(payload['error'])
    return payload['result']

if __name__=='__main__':
    endpoint='https://soroban-testnet.stellar.org'
    health=request(endpoint,'getHealth',{})
    config={'endpoint':endpoint,'start':health['latestLedger']-20,'end':health['latestLedger']+1,
            'filters':[{'type':'contract'}]}
    with tempfile.TemporaryDirectory() as temp:
        journal=Journal(str(Path(temp)/'pilot.sqlite'))
        try:
            first=collect(journal,config,request,page_limit=25,max_pages=100)
            second=collect(journal,config,request,page_limit=25,max_pages=100)
            if first['replayHash']!=second['replayHash']:raise RuntimeError('Replay mismatch')
            if first['coverage']!='rpc-range-exhausted':raise RuntimeError('Range not exhausted')
        finally:journal.close()
    destination=Path(sys.argv[1] if len(sys.argv)>1 else 'testnet-manifest.json')
    destination.write_text(json.dumps(first,indent=2))
    print(json.dumps({'network':'testnet','start':config['start'],'end':config['end'],
        'events':first['eventCount'],'pages':first['pages'],'replayHash':first['replayHash'],
        'secondReplayMatched':True,'coverage':first['coverage']},indent=2))
