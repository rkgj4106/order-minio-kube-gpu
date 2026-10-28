import asyncio
import json
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from io import BytesIO
from zoneinfo import ZoneInfo

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app import QueryPlan, Question, Ollama, create_app, execute_plan
from order_data import Order, Settings, read_parquet

KST = ZoneInfo('Asia/Seoul')
NOW = datetime(2026,10,9,16,10,tzinfo=KST)


def order(identifier='O1',customer='C001',product='P001',quantity=2,amount='20.00',age=10):
    return Order(identifier,customer,product,quantity,Decimal(amount),NOW-timedelta(seconds=age),NOW)


def test_kst_parquet_decimal_values():
    payload=BytesIO()
    pq.write_table(pa.Table.from_pylist([dict(order_id='O1',customer_id='C001',product_id='P001',quantity=1,
                                           total_amount=Decimal('0.10'),order_time=NOW.replace(tzinfo=None),etl_extracted_at=NOW)]),payload)
    rows=read_parquet(payload.getvalue(),KST)
    assert rows[0].ordered_at==NOW
    result=execute_plan(QueryPlan(),rows,NOW)
    assert result['summary']['amount']=='0.10'
    assert result['rows'][0]['evidence_id']=='R1'


def test_amount_is_decimal_sum_and_filters_are_applied():
    rows=[order('O1',amount='0.10'),order('O2',amount='0.20'),order('O3',customer='C002',amount='4')]
    result=execute_plan(QueryPlan(customer_ids=['C001']),rows,NOW)
    assert result['summary']==dict(orders=2,quantity=4,amount='0.30',customers=1,products=1)


def test_customer_ranking_top_limit_and_full_summary():
    rows=[order('O1',customer='C001',amount='10'),order('O2',customer='C002',amount='40')]
    result=execute_plan(QueryPlan(action='ranking',limit=1),rows,NOW)
    assert result['rows'][0]['entity']=='C002'
    assert result['summary']['amount']=='50.00'
    assert result['rows_total']==2 and result['truncated']


def test_product_ranking_uses_real_quantity_and_product_name():
    rows=[order('O1',quantity=2),order('O2',product='P002',quantity=3)]
    result=execute_plan(QueryPlan(action='ranking',metric='quantity',group_by='product'),rows,NOW)
    assert result['rows'][0]['entity']=='P002'
    assert result['rows'][0]['product_name']=='Monitor'


def test_yesterday_excludes_today_midnight_and_future_orders():
    rows=[order('Y',age=16*3600+601),order('today',age=16*3600+600),order('future',age=-60)]
    result=execute_plan(QueryPlan(period='yesterday'),rows,NOW)
    assert result['summary']['orders']==1


def test_trend_fills_empty_hours_and_reports_zero_baseline():
    rows=[order('prior',quantity=5,age=70*60),order('now',quantity=2,age=10)]
    result=execute_plan(QueryPlan(action='trend',period='today',metric='quantity',interval='hour',limit=20),rows,NOW)
    cells=result['rows']
    assert cells[-2]['quantity']==5 and cells[-1]['quantity']==2
    assert cells[-1]['delta']=='-3.00' and cells[-1]['delta_pct']=='-60.00'
    assert cells[0]['quantity']==0 and cells[0]['delta'] is None


def test_daily_trend_has_kst_midnight_boundaries():
    rows=[order('first',age=2*86400),order('second',age=86400),order('today',age=10)]
    result=execute_plan(QueryPlan(action='trend',period='all',interval='day'),rows,NOW)
    assert all(datetime.fromisoformat(r['time']).hour==0 for r in result['rows'])


def test_compare_uses_equal_length_previous_window_and_zero_pct_is_none():
    rows=[order('prev',quantity=2,age=4000),order('current',quantity=5,age=60)]
    result=execute_plan(QueryPlan(action='compare',period='last1h',metric='quantity'),rows,NOW)
    previous,current=result['rows']
    assert previous['quantity']==2 and current['quantity']==5
    assert current['delta']=='3.00' and current['delta_pct']=='150.00'
    result=execute_plan(QueryPlan(action='compare',period='last1h'),[order()],NOW)
    assert result['rows'][1]['delta_pct'] is None


def test_custom_range_uses_exclusive_end_and_naive_kst():
    rows=[order('in',age=20),order('end',age=10)]
    plan=QueryPlan(period='custom',start=(NOW-timedelta(seconds=30)).replace(tzinfo=None),end=(NOW-timedelta(seconds=10)).replace(tzinfo=None))
    result=execute_plan(plan,rows,NOW)
    assert result['summary']['orders']==1


def test_orders_are_sorted_newest_first_and_bound_to_limit():
    result=execute_plan(QueryPlan(action='orders',limit=1),[order('old',age=20),order('new',age=5)],NOW)
    assert result['rows'][0]['order_id']=='new'
    assert result['truncated'] and result['rows_total']==2


@pytest.mark.parametrize('payload',[
    {'action':'sql','sql':'DROP TABLE orders'},
    {'limit':500},
    {'action':'compare','period':'all'},
    {'period':'custom'},
    {'period':'custom','start':'2026-10-10T00:00:00+09:00','end':'2026-10-09T00:00:00+09:00'},
    {'period':'today','start':'2026-10-09T00:00:00+09:00'},
])
def test_invalid_plan_never_executes(payload):
    with pytest.raises(ValidationError): QueryPlan(**payload)


def test_future_range_is_not_a_historical_zero_answer():
    plan=QueryPlan(period='custom',start=NOW+timedelta(hours=1),end=NOW+timedelta(hours=2))
    with pytest.raises(ValueError,match='미래'): execute_plan(plan,[order()],NOW)


class Store:
    def __init__(self,rows=None,error=None,success=True):
        self.rows=tuple([order()] if rows is None else rows)
        self.state=dict(last_success=datetime.now(timezone.utc).isoformat() if success else None,
                        error=error,file_count=1,order_count=len(self.rows))
    def snapshot(self): return self.rows,dict(self.state)


class FakeLLM:
    model='gemma3:12b'
    def __init__(self,plan=None): self.query=plan or QueryPlan(period='all');self.explains=0
    async def models(self): return [self.model]
    async def plan(self,*args): return self.query
    async def explain(self,question,evidence):
        self.explains+=1
        return f"주문액은 {evidence['summary']['amount']}원입니다. [R1]"


def test_api_returns_answer_evidence_scope_and_status():
    llm=FakeLLM()
    with TestClient(create_app(Store(),llm,start_poller=False)) as client:
        assert client.get('/healthz').status_code==200
        assert client.get('/api/status').json()['ollama_error'] is None
        response=client.post('/api/chat',json={'question':'총 주문액은?'})
        assert response.status_code==200
        result=response.json()
        assert result['evidence']['summary']['amount']=='20.00'
        assert '[R1]' in result['answer']
        assert result['evidence']['timezone']=='Asia/Seoul'
        assert 'ASK YOUR ORDER DATA' in client.get('/').text


def test_empty_and_unknown_customer_do_not_ask_model_to_invent_results():
    llm=FakeLLM(QueryPlan(period='all',customer_ids=['unknown']))
    with TestClient(create_app(Store(),llm,start_poller=False)) as client:
        result=client.post('/api/chat',json={'question':'없는 고객의 주문'}).json()
        assert '없습니다' in result['answer'] and llm.explains==0
    with TestClient(create_app(Store(rows=[]),llm,start_poller=False)) as client:
        assert client.post('/api/chat',json={'question':'주문액'}).json()['evidence'] is None


def test_unsupported_question_and_initial_source_failure():
    llm=FakeLLM(QueryPlan(action='unsupported',reason='고객 주소는 데이터에 없습니다.'))
    with TestClient(create_app(Store(),llm,start_poller=False)) as client:
        response=client.post('/api/chat',json={'question':'고객 주소는?'})
        assert response.json()['answer']=='고객 주소는 데이터에 없습니다.'
        assert llm.explains==0
    with TestClient(create_app(Store(error='MinIO 접근 오류',success=False),llm,start_poller=False)) as client:
        assert client.post('/api/chat',json={'question':'주문액은?'}).status_code==503


def test_last_good_source_warns_and_model_failures_are_not_answers():
    with TestClient(create_app(Store(error='갱신 오류'),FakeLLM(),start_poller=False)) as client:
        assert client.post('/api/chat',json={'question':'주문액은?'}).json()['warning']
    class Failing(FakeLLM):
        async def plan(self,*_): raise RuntimeError('모델 태그를 확인하세요.')
    with TestClient(create_app(Store(),Failing(),start_poller=False)) as client:
        response=client.post('/api/chat',json={'question':'주문액은?'})
        assert response.status_code==502 and '모델' in response.json()['detail']


def test_ollama_real_request_format_and_json_plan_repair():
    sent=[]
    async def run():
        async def handler(request):
            if request.method=='GET': return httpx.Response(200,json={'models':[{'name':'gemma3:12b'}]})
            body=json.loads(request.content);sent.append(body)
            if len(sent)==1: text='{"action":"sql"}'
            elif len(sent)==2: text=QueryPlan(period='all').model_dump_json()
            else: text='총 주문액은 20원입니다. [R1]'
            return httpx.Response(200,json={'message':{'content':text}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            llm=Ollama(client,'http://ollama:11434','gemma3:12b')
            assert await llm.models()==['gemma3:12b']
            plan=await llm.plan(Question(question='총 주문액'),[order()],NOW)
            assert plan.period=='all'
            result=await llm.explain('주문액',execute_plan(plan,[order()],NOW))
            assert '[R1]' in result
    asyncio.run(run())
    assert sent[0]['format']['properties']['action']['enum']
    assert all(body['stream'] is False for body in sent)
    assert '직전 JSON' in sent[1]['messages'][0]['content']
    assert 'format' not in sent[2]


def test_ollama_404_explains_missing_model():
    async def run():
        async def handler(_): return httpx.Response(404,json={'error':"model 'gemma3:12b' not found"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(RuntimeError,match='not found'): await Ollama(client,'http://ollama','gemma3:12b').chat('질문')
    asyncio.run(run())


def test_second_request_returns_busy_instead_of_waiting_for_gpu():
    entered,released=threading.Event(),threading.Event()
    class Slow(FakeLLM):
        async def plan(self,*_):
            entered.set()
            await asyncio.to_thread(released.wait,5)
            return self.query
    with TestClient(create_app(Store(),Slow(),start_poller=False)) as client:
        replies=[]
        worker=threading.Thread(target=lambda:replies.append(client.post('/api/chat',json={'question':'첫 질문'})))
        worker.start()
        try:
            assert entered.wait(2)
            assert client.post('/api/chat',json={'question':'다음 질문'}).status_code==429
        finally:
            released.set();worker.join(5)
        assert replies[0].status_code==200
