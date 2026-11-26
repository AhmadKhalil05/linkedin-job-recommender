"""Offline tests for real HTTP accounting, backoff, SDK page budgeting, and cursors."""

from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path
import sys
import time
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"scraper"))
from rate_control import AdaptiveRate, HttpObserver, next_offset, retry_after_seconds
from scraper import fetch_jobs
from storage import Store


class Clock:
    def __init__(self):
        self.value=0.0
        self.waits=[]
    def now(self):
        return self.value
    def wait(self,seconds):
        self.waits.append(seconds)
        self.value+=seconds


def cards(count=10, malformed=0):
    return ''.join(f'''<div class="base-search-card" data-entity-urn="urn:li:jobPosting:{i}">
      {'' if i<malformed else '<h3 class="base-search-card__title">Test Engineer</h3>'}
      <h4 class="base-search-card__subtitle"><a href="https://example.com/company">Test Co</a></h4>
      <span class="job-search-card__location">Paris, France</span>
      <time datetime="2026-11-26"></time></div>''' for i in range(count))


class RateTests(unittest.TestCase):
    def test_retry_after_seconds_and_http_date(self):
        self.assertEqual(retry_after_seconds('1800'),1800)
        self.assertEqual(retry_after_seconds('-1'),0)
        self.assertEqual(retry_after_seconds('not-a-date'),0)
        date=format_datetime(datetime.fromtimestamp(3600,timezone.utc),usegmt=True)
        self.assertEqual(retry_after_seconds(date,now=1000),2600)

    def test_persistent_policy_increases_and_recovers_slowly(self):
        with TemporaryDirectory() as directory:
            store=Store(directory)
            policy=AdaptiveRate(store,600)
            self.assertEqual(policy.interval,600)
            self.assertGreaterEqual(policy.rate_limited(7200),7200)
            self.assertEqual(policy.interval,1200)
            for _ in range(4):
                self.assertEqual(policy.succeeded(),1200)
            self.assertEqual(AdaptiveRate(store,600).interval,1200)
            self.assertEqual(policy.succeeded(),1080)
            self.assertEqual(AdaptiveRate(store,600).interval,1080)

    def test_throttling_is_not_treated_as_end_of_results(self):
        self.assertEqual(next_offset(200,{'records':[],'rate_limited':True},10,40),200)
        self.assertEqual(next_offset(200,{'records':[{},{}],'rate_limited':True},10,40),202)
        self.assertEqual(next_offset(200,{'records':[],'errors':{'linkedin':'timeout'}},10,40),200)
        self.assertEqual(next_offset(200,{'records':[],'search_cards':0},10,40),0)
        self.assertEqual(next_offset(200,{'records':[{}]*10,'search_cards':10},10,40),210)

    def test_sdk_25_results_is_reduced_to_one_10_card_page(self):
        import requests
        calls=[]
        def send(session,request,**kwargs):
            calls.append(request.url)
            response=requests.Response()
            response.status_code=200
            response.url=request.url
            response.request=request
            response._content=cards().encode()
            return response
        with patch('requests.sessions.Session.send',new=send):
            result=fetch_jobs({'keyword':'Engineer','location':'France','batch_size':25,
                'offset':0,'hours_old':72,'fetch_description':False})
        self.assertEqual(len(calls),1)
        self.assertEqual(len(result['records']),10)
        self.assertEqual(result['http_calls'],1)
        self.assertEqual(result['effective_batch_size'],10)
        self.assertFalse(result['rate_limited'])

    def test_bad_cards_do_not_trigger_an_extra_network_page(self):
        import requests
        calls=[]
        def send(session,request,**kwargs):
            calls.append(request.url)
            response=requests.Response()
            response.status_code=200
            response.url=request.url
            response.request=request
            response._content=cards(malformed=2).encode()
            return response
        with patch('requests.sessions.Session.send',new=send), patch('jobspy.linkedin.time.sleep'):
            result=fetch_jobs({'keyword':'Engineer','location':'France','batch_size':10,
                'offset':0,'hours_old':72,'fetch_description':False})
        self.assertEqual(len(calls),1)
        self.assertEqual(len(result['records']),8)
        self.assertTrue(result['budget_deferred'])
        self.assertFalse(result['errors'])
        self.assertEqual(next_offset(0,result,10,20),10)

    def test_detail_429_stops_remaining_http_and_captures_retry_after(self):
        import requests
        clock=Clock()
        observer=HttpObserver(clock=clock.now,sleeper=clock.wait,wall_clock=clock.now)
        calls=[]
        def send(session,request,**kwargs):
            calls.append((request.url,clock.value))
            response=requests.Response()
            response.url=request.url
            response.request=request
            response.status_code=200 if 'seeMoreJobPostings' in request.url else 429
            response._content=cards().encode() if response.status_code==200 else b'Too many requests'
            if response.status_code==429:
                response.headers['Retry-After']='1800'
            return response
        with patch('requests.sessions.Session.send',new=send), patch('scraper.HttpObserver',return_value=observer):
            result=fetch_jobs({'keyword':'Engineer','location':'France','batch_size':10,
                'offset':40,'hours_old':72,'fetch_description':True})
        self.assertEqual(len(calls),2)
        self.assertGreaterEqual(calls[1][1]-calls[0][1],30)
        self.assertEqual(result['http_calls'],2)
        self.assertEqual(result['http_429'],1)
        self.assertTrue(result['rate_limited'])
        self.assertEqual(result['retry_after_seconds'],1800)
        self.assertEqual(len(result['records']),10)
        self.assertEqual(next_offset(40,result,10,20),50)

    def test_rate_report_distinguishes_measured_and_unknown_counts(self):
        import importlib.util
        spec=importlib.util.spec_from_file_location('rate_report',ROOT/'scripts'/'rate_report.py')
        module=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with TemporaryDirectory() as directory:
            store=Store(directory)
            store.set_meta('live_rate_observations',[
                {'started_at':time.time()-3600,'http_calls':2,'http_429':1,'rate_limited':True,
                 'source_error':None,'events':10,'new_jobs':9},
                {'started_at':time.time()-100,'http_calls':None,'http_429':None,'rate_limited':False,
                 'source_error':'timeout','events':0,'new_jobs':0},
            ])
            result=module.report(store,2)
            self.assertEqual(result['measured_http_calls'],2)
            self.assertEqual(result['measured_http_429_percent'],50)
            self.assertEqual(result['unmeasured_batches'],1)
            self.assertEqual(result['new_unique_jobs'],9)
            self.assertIn('do not accelerate',result['assessment'])


if __name__=='__main__':
    unittest.main(verbosity=2)
