# -*- coding: utf-8 -*-
# Copyright (c) 2016, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
import json
from .exceptions import QuickbooksSetupError

def disable_quickbooks_sync_on_exception():
	frappe.db.rollback()
	frappe.db.set_value("Quickbooks Settings", None, "enable_quickbooks_online", 0)
	frappe.db.commit()
	
def make_quickbooks_log(title="Sync Log", status="Queued", method="sync_quickbooks", message=None, exception=False, 
name=None, request_data={}):	
	if not name:
		name = frappe.db.get_value("Quickbooks Log", {"status": "Queued"})
		
		if name:
			""" if name not provided by log calling method then fetch existing queued state log"""
			log = frappe.get_doc("Quickbooks Log", name)
		
		else:
			""" if queued job is not found create a new one."""
			log = frappe.get_doc({"doctype":"Quickbooks Log"}).insert(ignore_permissions=True)
		
		if exception:
			frappe.db.rollback()
			log = frappe.get_doc({"doctype":"Quickbooks Log"}).insert(ignore_permissions=True)
			
		log.message = message if message else frappe.get_traceback()
		log.title = title[0:140]
		log.method = method
		log.status = status
		log.request_data= json.dumps(request_data)
		
		log.save(ignore_permissions=True)
		frappe.db.commit()

def pagination(quickbooks_obj, business_objects):
	condition = ""
	group_by = ""
	quickbooks_result_set = []
	if business_objects in ["Customer", "Vendor", "Item", "Employee"]:
		condition = " Where Active IN (true, false)"
		
	record_count = quickbooks_obj.query("""SELECT count(*) from {0} {1} """.format(business_objects, condition))
	total_record = record_count['QueryResponse']['totalCount']
	limit_count = 90
	total_page = total_record / limit_count if total_record % limit_count == 0 else total_record / limit_count + 1
	startposition , maxresults = 0, 0  
	for i in range(total_page):
		maxresults = startposition + limit_count
		if business_objects in ["Customer", "Vendor", "Item", "Employee"]:
			group_by = condition + " ORDER BY Id ASC STARTPOSITION {1} MAXRESULTS {2}".format(business_objects, startposition, maxresults)
		else:
			group_by = " ORDER BY Id ASC STARTPOSITION {1} MAXRESULTS {2}".format(business_objects, startposition, maxresults)
		query_result = """SELECT * FROM {0} {1}""".format(business_objects, group_by)
		qb_data = quickbooks_obj.query(query_result)
		qb_result =  qb_data['QueryResponse']
		if qb_result:
			quickbooks_result_set.extend(qb_result[business_objects])
		startposition = startposition + limit_count
	return quickbooks_result_set

def cancel_record(quickbooks_obj):
	"""
		Cancel record in Erpnext which are deleted in QuickBooks
	"""
	mapper =  frappe._dict({
			"Payment": ["quickbooks_payment_id", "Payment Entry", "SI"],
			"BillPayment": ["quickbooks_payment_id", "Payment Entry", "PI"]
		})
	quickbooks_business_objects = ["Payment", "BillPayment"]

	for business_objects in quickbooks_business_objects:
		query = """
					select `{0}` from `tab{1}`""".format(mapper[business_objects][0], mapper[business_objects][1])
		quickbooks_business_objects_ids = frappe.db.sql(query)
		if quickbooks_business_objects_ids:
			erp_synced_obj_ids = [id.split("-")[0] for id in  [row[0] for row in quickbooks_business_objects_ids] if id.split("-")[1] == mapper[business_objects][2]]
			qb_obj_ids = qb_deleted_record(quickbooks_obj, business_objects)
			deleted_record_from_qb = [row+"-"+mapper[business_objects][2] for row in erp_synced_obj_ids if row not in qb_obj_ids]
			for qb_obj_ids in deleted_record_from_qb:
				query = """
					select name, quickbooks_payment_id from `tab{0}` 
					where quickbooks_payment_id like '%{1}%' and docstatus != 2 """.format(mapper[business_objects][1], qb_obj_ids)
				doc_name = frappe.db.sql(query, as_dict=1)
				if doc_name:
					frappe.get_doc(mapper[business_objects][1], doc_name[0]["name"]).cancel()
					frappe.db.commit()

def qb_deleted_record(quickbooks_obj, business_objects):
	""" Unlike as Erpnext, Quickbooks also support hard delete. 
		It only support soft delete for masters not for entries.
		So function is to cancel the entried in Erpnext which are not Soft-deleted in Quickbooks.  
	"""
	condition, group_by = "", ""
	quickbooks_result_set = []
	if business_objects in ["Payment", "BillPayment"]:
		record_count = quickbooks_obj.query("""SELECT count(*) from {0}""".format(business_objects))
		total_record = record_count['QueryResponse']['totalCount']
		limit_count = 90
		total_page = total_record / limit_count if total_record % limit_count == 0 else total_record / limit_count + 1
		startposition , maxresults = 0, 0  

		for i in range(total_page):
			maxresults = startposition + limit_count
			if business_objects in ["Payment", "BillPayment"]:
				group_by =" ORDER BY Id ASC STARTPOSITION {1} MAXRESULTS {2}".format(business_objects, startposition, maxresults)
			query_result = """SELECT Id FROM {0} {1}""".format(business_objects, group_by)
			qb_data = quickbooks_obj.query(query_result)
			qb_result =  qb_data['QueryResponse']
			if qb_result:
				ids = []
				for row in qb_result[business_objects]:
					ids.append(row.get("Id"))
				quickbooks_result_set.extend(ids)
			startposition = startposition + limit_count
	return quickbooks_result_set