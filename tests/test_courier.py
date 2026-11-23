from datetime import datetime, timedelta
from unittest import TestCase
from unittest.mock import patch
import hashlib
import hmac
import json
import time
from urllib.parse import urlencode

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.app as site
from app import courier, courier_bot


class CourierTests(TestCase):
    def setUp(self):
        engine = create_engine("sqlite://")
        site.Base.metadata.create_all(engine)
        self.db = sessionmaker(bind=engine)
        self.addCleanup(engine.dispose)
        self.addCleanup(patch.stopall)
        patch.object(site, "SessionLocal", self.db).start()
        patch.object(site, "track_daily_guest_visit").start()
        self.admin = site.app.test_client()
        self.phone = site.app.test_client()
        with self.admin.session_transaction() as session:
            session.update(admin_auth=True, marketplace_csrf="known")
        with self.db.begin() as db:
            db.add(site.Warehouse(id=1, name="Ревізія"))
            db.flush()
            db.add(site.Part(id=1, part_number="OEM123", warehouse_id=1, storage_location="A-04",
                name="Фара", description="Права", photo_urls="/uploads/part.jpg", qty=10, in_stock=True))
            for i in (1, 2, 3):
                order = site.Order(id=i, phone="123", customer_name="Тест", city="Ромни", np_warehouse_label="Відділення №1",
                    delivery_type="nova_poshta", external_source="prom" if i==2 else "", external_order_id="222" if i==2 else "",
                    stock_reserved=i==2, status="new")
                order.items.append(site.OrderItem(part_id=1, part_number="OEM123", name="Фара", qty=2))
                db.add(order)

    def pair(self):
        result = self.admin.post("/admin/courier", data={"csrf_token":"known","action":"web"})
        from html.parser import HTMLParser
        class Inputs(HTMLParser):
            code = None
            def handle_starttag(self, tag, attrs):
                a = dict(attrs)
                if tag == "input" and a.get("value","").startswith("https://usaparts.top/courier/#invite="):
                    self.code = a["value"].split("=")[-1]
        parser = Inputs()
        parser.feed(result.text)
        self.assertTrue(parser.code)
        self.assertTrue(self.phone.post("/courier/login", json={"code":parser.code}).json["ok"])
        self.assertEqual(self.phone.post("/courier/login", json={"code":parser.code}).status_code, 403)

    def handoff(self, i=1, payment="0"):
        return self.admin.post(f"/admin/orders/{i}/courier", data={"csrf_token":"known","control_payment_uah":payment})

    def save(self, i, number, version=None):
        card = self.phone.get(f"/courier/orders/{i}").json
        return self.phone.post(f"/courier/orders/{i}", json={"ttn":number, "version":version or card["card"]["version"]},
                               headers={"X-Courier-CSRF":card["csrf"]})

    def test_explicit_payment_authorization_and_single_reserve(self):
        self.assertEqual(self.phone.get("/courier/list").status_code, 401)
        self.assertEqual(self.admin.post("/admin/orders/1/courier").status_code,403)
        for value in ("", "-2", "NaN", "1.111","10000000000"):
            self.handoff(payment=value)
        with self.db() as db:
            self.assertIsNone(db.get(courier.MODELS.Dispatch, 1))
        self.handoff(payment="0")
        self.handoff(payment="150")
        self.pair()
        self.assertEqual(self.phone.get("/admin/orders").status_code,302)
        self.assertEqual(self.phone.get("/courier/list").json["rows"][0]["id"],1)
        card = self.phone.get("/courier/orders/1").json["card"]
        self.assertEqual(card["items"][0]["location"],"A-04")
        self.assertEqual(card["items"][0]["oem"],"OEM123")
        self.assertEqual(card["items"][0]["qty"],2)
        self.assertEqual(card["items"][0]["description"],"Права")
        self.assertTrue(card["items"][0]["photo"])
        self.assertEqual(card["payment"],"0.00")
        self.assertEqual(self.phone.get("/courier/orders/3").status_code,404)
        with self.db() as db:
            self.assertEqual(db.get(site.Part,1).qty,8)
        self.admin.post("/admin/courier",data={"action":"revoke","csrf_token":"known"})
        self.assertEqual(self.phone.get("/courier/list").status_code,401)

    def test_waybill_conflict_archive_and_prom(self):
        self.handoff()
        self.handoff(2,"125,30")
        self.pair()
        url="/courier/orders/2"
        data=self.phone.get(url).json
        self.assertEqual(self.phone.post(url,json={"ttn":"20000000000009"}).status_code,403)
        for number, version in (("123",None),("20000000000009","old")):
            self.assertEqual(self.save(2,number,version).status_code,409)
        self.assertTrue(self.save(2,"20000000000009").json["ok"])
        self.assertTrue(self.save(2,"20000000000009").json["ok"])
        self.assertEqual(self.save(2,"20000000000008").status_code,409)
        self.assertEqual(self.save(1,"20000000000009").status_code,409)
        self.assertEqual([r["id"] for r in self.phone.get("/courier/list").json["rows"]],[1])
        self.assertEqual([r["id"] for r in self.phone.get("/courier/list?tab=archive").json["rows"]],[2])
        with self.db() as db:
            self.assertEqual(db.get(site.Order,2).status,"shipped")
            self.assertEqual(db.get(site.marketplace_models.Waybill,2).number,"20000000000009")
            self.assertEqual(db.get(site.Part,1).qty,8)

    def test_cancellation_in_open_card(self):
        self.handoff()
        self.pair()
        old = self.phone.get("/courier/orders/1").json
        with self.db.begin() as db:
            db.get(site.Order,1).external_status="canceled"
        result = self.phone.post("/courier/orders/1",json={"ttn":"20000000000001","version":old["card"]["version"]},
                                 headers={"X-Courier-CSRF":old["csrf"]})
        self.assertEqual(result.status_code,409)
        self.assertEqual(self.phone.get("/courier/orders/1").status_code,404)
        self.assertEqual(self.phone.get("/courier/list").json["rows"],[])

    def test_manual_shipment_legacy_isolation(self):
        self.admin.post("/admin/packing/create",data={"csrf_token":"known","items_json":'[{"partId":1,"qty":2}]',
            "delivery_type":"nova_poshta","control_payment_uah":"12.50","city_name":"Ромни","warehouse_label":"№1"})
        with self.db() as db:
            dispatch=db.query(courier.MODELS.Dispatch).one()
            order=db.get(site.Order,dispatch.order_id)
            self.assertEqual(order.status,"awaiting_shipment")
            self.assertEqual(str(dispatch.payment),"12.50")
            packing=db.query(site.PackingRequest).one()
            rid=packing.id
        self.assertEqual(self.phone.get("/api/mobile/shipment/list").json["requests"],[])
        self.assertEqual(self.phone.post(f"/api/mobile/shipment/{rid}/ttn",json={"ttn":"20000000000009"}).status_code,409)

    def test_bot_private_pairing_notices_and_item_photos(self):
        self.handoff()
        with self.db() as db, patch.object(courier_bot,"api",return_value={"message_id":13}) as api:
            cfg=courier.config(db)
            cfg.bot_token=site.marketplaces.cipher().encrypt(b"token").decode()
            cfg.invite_channel="tg";cfg.invite_until=datetime.utcnow()+timedelta(minutes=1);cfg.invite_hash=courier.digest("invite")
            def update(chat, text):
                return {"message":{"from":{"id":45},"chat":{"type":chat,"id":45},"text":text}}
            courier_bot.process(db,cfg,"token",update("group","/start invite"))
            self.assertFalse(api.called)
            courier_bot.process(db,cfg,"token",update("private","/start invite"))
            self.assertEqual(cfg.chat_id,"45")
            courier_bot.notices(db,cfg,"token")
            text=api.call_args.args[2]["text"]
            for field in ("OEM123","A-04","0.00 грн","2 шт.","№1"):
                self.assertIn(field,text)
            api.reset_mock()
            courier_bot.notices(db,cfg,"token")
            self.assertFalse(api.called)
            cb={"from":{"id":45},"id":"1","message":{"chat":{"type":"private","id":45}},"data":"items:1:0"}
            courier_bot.process(db,cfg,"token",{"callback_query":cb})
            self.assertEqual(api.call_args.args[1],"sendPhoto")
            self.assertIn("Права",api.call_args.args[2]["caption"])
            db.get(site.Order,1).ttn="20000000000009"
            courier_bot.notices(db,cfg,"token")
            self.assertEqual(api.call_args.args[1],"editMessageText")
            self.assertIn("Архів",api.call_args.args[2]["text"])

    def test_connect_named_bot_and_keep_credential_private(self):
        token = "123456789:" + "A" * 35
        with patch.object(courier, "telegram_api", side_effect=[
            {"username":"USAparts_delivery_bot","first_name":"Доставка товару"}, {"url":""}
        ]) as api:
            result = self.admin.post("/admin/courier",data={"action":"bot","csrf_token":"known","token":token})
        self.assertIn("@USAparts_delivery_bot",result.text)
        self.assertNotIn(token,result.text)
        self.assertEqual([call.args[1] for call in api.call_args_list], ["getMe", "getWebhookInfo"])
        with self.db() as db:
            cfg = courier.config(db)
            self.assertNotIn(token,cfg.bot_token)
            self.assertEqual(courier.bot_token(cfg),token)

    def test_telegram_signed_launch(self):
        def payload(stamp):
            fields={"auth_date":str(stamp),"user":json.dumps({"id":45})}
            key=hmac.digest(b"WebAppData",b"token","sha256")
            fields["hash"]=hmac.new(key,"\n".join(f"{k}={v}" for k,v in sorted(fields.items())).encode(),hashlib.sha256).hexdigest()
            return urlencode(fields)
        good=payload(int(time.time()))
        self.assertEqual(courier.telegram_user(good,"token"),"45")
        self.assertEqual(courier.telegram_user(good,"another"),"")
        self.assertEqual(courier.telegram_user(payload(10),"token"),"")
