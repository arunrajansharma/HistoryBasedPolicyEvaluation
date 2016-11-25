
import da
PatternExpr_346 = da.pat.TuplePattern([da.pat.ConstantPattern('done')])
PatternExpr_351 = da.pat.FreePattern('p')
PatternExpr_370 = da.pat.TuplePattern([da.pat.ConstantPattern('done')])
PatternExpr_375 = da.pat.FreePattern('p')
PatternExpr_387 = da.pat.TuplePattern([da.pat.ConstantPattern('policyDecisionFromSub_Co'), da.pat.FreePattern('decision')])
PatternExpr_394 = da.pat.FreePattern('sub_coord_id')
PatternExpr_461 = da.pat.TuplePattern([da.pat.ConstantPattern('done')])
PatternExpr_466 = da.pat.FreePattern('p')
PatternExpr_485 = da.pat.TuplePattern([da.pat.ConstantPattern('done')])
PatternExpr_490 = da.pat.FreePattern('p')
PatternExpr_502 = da.pat.TuplePattern([da.pat.ConstantPattern('evalRequestFromApp'), da.pat.FreePattern('req')])
PatternExpr_509 = da.pat.FreePattern('app_id')
PatternExpr_633 = da.pat.TuplePattern([da.pat.ConstantPattern('decisionFromWorker'), da.pat.FreePattern('req')])
PatternExpr_640 = da.pat.FreePattern('w_id')
PatternExpr_830 = da.pat.TuplePattern([da.pat.ConstantPattern('conflictEvalReplyFromRes_Co'), da.pat.FreePattern('conflict_decision'), da.pat.FreePattern('req')])
PatternExpr_839 = da.pat.FreePattern('res_coord_id')
PatternExpr_1016 = da.pat.TuplePattern([da.pat.ConstantPattern('done')])
PatternExpr_1021 = da.pat.FreePattern('p')
PatternExpr_1040 = da.pat.TuplePattern([da.pat.ConstantPattern('done')])
PatternExpr_1045 = da.pat.FreePattern('p')
PatternExpr_1057 = da.pat.TuplePattern([da.pat.ConstantPattern('evalRequestFromSub_Co'), da.pat.FreePattern('req')])
PatternExpr_1064 = da.pat.FreePattern('sub_coord_id')
PatternExpr_1132 = da.pat.TuplePattern([da.pat.ConstantPattern('conflictEvalRequestFromSub_Co'), da.pat.FreePattern('req')])
PatternExpr_1139 = da.pat.FreePattern('sub_coord_id')
PatternExpr_1270 = da.pat.TuplePattern([da.pat.ConstantPattern('subAttrsFromDB'), da.pat.FreePattern('sub_attr_dict')])
PatternExpr_1276 = da.pat.FreePattern('dbEmulator')
PatternExpr_1296 = da.pat.TuplePattern([da.pat.ConstantPattern('resAttrsFromDB'), da.pat.FreePattern('res_attr_dict')])
PatternExpr_1302 = da.pat.FreePattern('dbEmulator')
PatternExpr_1672 = da.pat.TuplePattern([da.pat.ConstantPattern('done')])
PatternExpr_1677 = da.pat.FreePattern('p')
PatternExpr_1696 = da.pat.TuplePattern([da.pat.ConstantPattern('done')])
PatternExpr_1701 = da.pat.FreePattern('p')
PatternExpr_1713 = da.pat.TuplePattern([da.pat.ConstantPattern('evalRequestFromRes_Co'), da.pat.FreePattern('req')])
PatternExpr_1720 = da.pat.FreePattern('res_coord_id')
PatternExpr_1837 = da.pat.TuplePattern([da.pat.ConstantPattern('updateFromDB'), da.pat.FreePattern('sub_attr_dict'), da.pat.FreePattern('res_attr_dict')])
PatternExpr_1844 = da.pat.FreePattern('dbEmulator')
PatternExpr_2110 = da.pat.TuplePattern([da.pat.ConstantPattern('done')])
PatternExpr_2115 = da.pat.FreePattern('p')
PatternExpr_2134 = da.pat.TuplePattern([da.pat.ConstantPattern('done')])
PatternExpr_2139 = da.pat.FreePattern('Master')
PatternExpr_2167 = da.pat.TuplePattern([da.pat.ConstantPattern('getSubAttrs'), da.pat.FreePattern('sub_attrs'), da.pat.FreePattern('sub_id')])
PatternExpr_2176 = da.pat.FreePattern('w_id')
PatternExpr_2196 = da.pat.TuplePattern([da.pat.ConstantPattern('getResAttrs'), da.pat.FreePattern('res_attrs'), da.pat.FreePattern('res_id')])
PatternExpr_2205 = da.pat.FreePattern('w_id')
PatternExpr_2225 = da.pat.TuplePattern([da.pat.ConstantPattern('updateSubAttrs'), da.pat.FreePattern('sub_attrs'), da.pat.FreePattern('sub_id')])
PatternExpr_2234 = da.pat.FreePattern('sub_coord_id')
PatternExpr_2298 = da.pat.TuplePattern([da.pat.ConstantPattern('updateResAttrs'), da.pat.FreePattern('res_attrs'), da.pat.FreePattern('res_id')])
PatternExpr_2307 = da.pat.FreePattern('res_coord_id')
PatternExpr_2825 = da.pat.TuplePattern([da.pat.ConstantPattern('okay')])
PatternExpr_2830 = da.pat.FreePattern('p')
PatternExpr_2866 = da.pat.TuplePattern([da.pat.ConstantPattern('okay')])
PatternExpr_2871 = da.pat.FreePattern('p')
_config_object = {}
import sys
import time
import random
import logging
import configparser
import xml.etree.ElementTree as ET

class Request():

    def __init__(self, sub_id, res_id, h, sub_attrs=None, res_attrs=None, action=None):
        self.app_id = None
        self.sub_id = sub_id
        self.res_id = res_id
        self.attrs_read_from_tent = {}
        self.attrs_read_from_cache = {}
        self.timestamp = time.time()
        self.sub_attrs = sub_attrs
        self.res_attrs = res_attrs
        self.sub_attrs_to_update = []
        self.res_attrs_to_update = []
        self.sub_attrs_for_policy_eval = {}
        self.hashMap = h
        self.action = action
        self.dbEmulator = None

class Rule():

    def __init__(self, rulename, sub_id, res_id, action, sub_attrs, res_attrs, sub_attrs_to_update, res_attrs_to_update):
        self.rulename = rulename
        self.dbEmulator = None
        self.sub_id = sub_id
        self.res_id = res_id
        self.sub_attrs = sub_attrs
        self.res_attrs = res_attrs
        self.action = action
        self.sub_attrs_to_update = sub_attrs_to_update
        self.res_attrs_to_update = res_attrs_to_update

class Application(da.DistProcess):

    def __init__(self, procimpl, props):
        super().__init__(procimpl, props)
        self._ApplicationReceivedEvent_0 = []
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_ApplicationReceivedEvent_0', PatternExpr_346, sources=[PatternExpr_351], destinations=None, timestamps=None, record_history=True, handlers=[]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ApplicationReceivedEvent_1', PatternExpr_370, sources=[PatternExpr_375], destinations=None, timestamps=None, record_history=None, handlers=[self._Application_handler_369]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ApplicationReceivedEvent_2', PatternExpr_387, sources=[PatternExpr_394], destinations=None, timestamps=None, record_history=None, handlers=[self._Application_handler_386])])

    def setup(self, hashMap, sub_co, res_co, master_id, dbEmulator, req):
        self._state.hashMap = hashMap
        self._state.sub_co = sub_co
        self._state.res_co = res_co
        self._state.master_id = master_id
        self._state.dbEmulator = dbEmulator
        self._state.req = req
        pass

    def run(self):
        self._state.req.dbEmulator = self._state.dbEmulator
        self.output(('Application sending Req to Sub_Co ' + str(self._state.req.hashMap[self._state.req.sub_id])))
        self._send(('evalRequestFromApp', self._state.req), self._state.req.hashMap[self._state.req.sub_id])
        super()._label('_st_label_341', block=False)
        _st_label_341 = 0
        while (_st_label_341 == 0):
            _st_label_341 += 1
            if (len([p for (_, (_, _, p), (_ConstantPattern362_,)) in self._ApplicationReceivedEvent_0 if (_ConstantPattern362_ == 'done')]) == 1):
                _st_label_341 += 1
            else:
                super()._label('_st_label_341', block=True)
                _st_label_341 -= 1

    def _Application_handler_369(self, p):
        self.output((str(self.id) + ' shutting Down'))
    _Application_handler_369._labels = None
    _Application_handler_369._notlabels = None

    def _Application_handler_386(self, decision, sub_coord_id):
        self.output(('Received Policy decision from Sub_Co at App-> ' + str(self.id)))
        if (decision == 'Success'):
            self.output('Policy Evaluated')
        self._send(('okay',), self._state.master_id)
    _Application_handler_386._labels = None
    _Application_handler_386._notlabels = None

class Sub_Co(da.DistProcess):

    def __init__(self, procimpl, props):
        super().__init__(procimpl, props)
        self._Sub_CoReceivedEvent_0 = []
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_Sub_CoReceivedEvent_0', PatternExpr_461, sources=[PatternExpr_466], destinations=None, timestamps=None, record_history=True, handlers=[]), da.pat.EventPattern(da.pat.ReceivedEvent, '_Sub_CoReceivedEvent_1', PatternExpr_485, sources=[PatternExpr_490], destinations=None, timestamps=None, record_history=None, handlers=[self._Sub_Co_handler_484]), da.pat.EventPattern(da.pat.ReceivedEvent, '_Sub_CoReceivedEvent_2', PatternExpr_502, sources=[PatternExpr_509], destinations=None, timestamps=None, record_history=None, handlers=[self._Sub_Co_handler_501]), da.pat.EventPattern(da.pat.ReceivedEvent, '_Sub_CoReceivedEvent_3', PatternExpr_633, sources=[PatternExpr_640], destinations=None, timestamps=None, record_history=None, handlers=[self._Sub_Co_handler_632]), da.pat.EventPattern(da.pat.ReceivedEvent, '_Sub_CoReceivedEvent_4', PatternExpr_830, sources=[PatternExpr_839], destinations=None, timestamps=None, record_history=None, handlers=[self._Sub_Co_handler_829])])

    def setup(self):
        self._state.procs = dict()
        self._state.app_id = None
        self._state.updated_cache = dict()
        self._state.has_read_from_tent = []
        self._state.tent_updated_attrs = {}

    def run(self):
        super()._label('_st_label_456', block=False)
        _st_label_456 = 0
        while (_st_label_456 == 0):
            _st_label_456 += 1
            if (len([p for (_, (_, _, p), (_ConstantPattern477_,)) in self._Sub_CoReceivedEvent_0 if (_ConstantPattern477_ == 'done')]) == 1):
                _st_label_456 += 1
            else:
                super()._label('_st_label_456', block=True)
                _st_label_456 -= 1

    def performCleanup(self, req):
        req.attrs_read_from_tent = {}
        req.attrs_read_from_cache = {}
        req.timestamp = time.time()
        req.sub_attrs_to_update = []
        req.res_attrs_to_update = []
        req.sub_attrs_for_policy_eval = {}
        return req

    def _Sub_Co_handler_484(self, p):
        self.output((str(self.id) + ' shutting Down'))
    _Sub_Co_handler_484._labels = None
    _Sub_Co_handler_484._notlabels = None

    def _Sub_Co_handler_501(self, req, app_id):
        self.output(('Received Eval Req from App at Sub_Co-> ' + str(self.id)))
        if (req.app_id is None):
            req.app_id = app_id
        self._state.procs[req] = 'Running'
        self.output(('reading tent' + str(self._state.tent_updated_attrs)))
        for attr in req.sub_attrs:
            if (attr in self._state.tent_updated_attrs):
                req.attrs_read_from_tent[attr] = self._state.tent_updated_attrs[attr]['value']
            elif (attr in self._state.updated_cache):
                req.attrs_read_from_cache[attr] = self._state.updated_cache[attr]
        self.output('Request Updated with Tent and Cache')
        self.output(('Final Req Object is-> ' + str(req)))
        self.output(('Sub_Co Sending Eval Request to Res_Co-> ' + str(req.hashMap[req.res_id])))
        self._send(('evalRequestFromSub_Co', req), req.hashMap[req.res_id])
    _Sub_Co_handler_501._labels = None
    _Sub_Co_handler_501._notlabels = None

    def _Sub_Co_handler_632(self, req, w_id):
        self.output(('Decision from Worker Recvd at-> ' + str(self.id)))
        self.output(('Sub obligations-> ' + str(req.sub_attrs_to_update)))
        self.output(('Res obligations-> ' + str(req.res_attrs_to_update)))
        conflict = None
        self.output(('Tent_attr BEFORE-> ' + str(self._state.tent_updated_attrs)))
        self.output(('My Time Stamp - ' + str(req.timestamp)))
        if req.sub_attrs_for_policy_eval:
            for attr in req.sub_attrs_for_policy_eval:
                if (attr in self._state.tent_updated_attrs):
                    if (not (req.sub_attrs_for_policy_eval[attr] == self._state.tent_updated_attrs[attr]['value'])):
                        conflict = 'Present'
                        self._state.procs[self.id] = 'Restarted'
                        self.output(('Subject Conflict Found. Restart-> ' + str(req.app_id)))
                        req = self.performCleanup(req)
                        self._send(('evalRequestFromApp', req), self.id)
                        break
        if (not (conflict == 'Present')):
            self.output('Subject Conflict-> Absent.')
            if req.sub_attrs_to_update:
                for attr in req.sub_attrs_to_update:
                    if (attr in self._state.tent_updated_attrs):
                        self._state.tent_updated_attrs[attr]['value'] = req.sub_attrs_to_update[attr]
                        self._state.tent_updated_attrs[attr]['timestamp'] = req.timestamp
                    else:
                        val = req.sub_attrs_to_update[attr]
                        ts = req.timestamp
                        self._state.tent_updated_attrs[attr] = dict()
                        self._state.tent_updated_attrs[attr]['value'] = val
                        self._state.tent_updated_attrs[attr]['timestamp'] = ts
            self.output(('Updated the tent_attr-> ' + str(self._state.tent_updated_attrs)))
            self.output(('Sending Req for Res Conflict eval to Res_Co-> ' + str(req.hashMap[req.res_id])))
            self._send(('conflictEvalRequestFromSub_Co', req), req.hashMap[req.res_id])
    _Sub_Co_handler_632._labels = None
    _Sub_Co_handler_632._notlabels = None

    def _Sub_Co_handler_829(self, conflict_decision, req, res_coord_id):
        self.output(('Received Conflict eval reply from Res_Co at-> ' + str(self.id)))
        if (conflict_decision == 'Present'):
            self.output('Resource Conflict was found at Res_Co.')
            for r in self._state.has_read_from_tent:
                if (r.timestamp > req.timestamp):
                    self.output(('Clearing Adminstration for request-> ' + str(r)))
                    for attr in r.sub_attrs:
                        if (attr in self._state.tent_updated_attrs):
                            if (self._state.tent_updated_attrs[attr]['timestamp'] == r.timestamp):
                                del self._state.tent_updated_attrs[attr]
                    self.output(('Restarting request ' + str(r)))
                    self._send(('evalRequestFromApp', r), self.id)
            self.output(('Clearing Adminstration for request-> ' + str(req)))
            for attr in req.sub_attrs:
                if (attr in self._state.tent_updated_attrs):
                    if (self._state.tent_updated_attrs[attr]['timestamp'] == req.timestamp):
                        del self._state.tent_updated_attrs[attr]
            self.output(('Restarting request-> ' + str(req)))
            self._send(('evalRequestFromApp', req), self.id)
        else:
            self.output('No Conflict. Going to Commit.')
            if req.sub_attrs_to_update:
                for attr in req.sub_attrs_to_update:
                    self._state.updated_cache[attr] = req.sub_attrs_to_update[attr]
            self.output('Sub_Co Committing updates to DB')
            self._send(('updateSubAttrs', self._state.updated_cache, req.sub_id), req.dbEmulator)
            self.output(('Sending Policy Decision from Sub_Co to App-> ' + str(req.app_id)))
            self._send(('policyDecisionFromSub_Co', 'Success'), req.app_id)
    _Sub_Co_handler_829._labels = None
    _Sub_Co_handler_829._notlabels = None

class Res_Co(da.DistProcess):

    def __init__(self, procimpl, props):
        super().__init__(procimpl, props)
        self._Res_CoReceivedEvent_0 = []
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_Res_CoReceivedEvent_0', PatternExpr_1016, sources=[PatternExpr_1021], destinations=None, timestamps=None, record_history=True, handlers=[]), da.pat.EventPattern(da.pat.ReceivedEvent, '_Res_CoReceivedEvent_1', PatternExpr_1040, sources=[PatternExpr_1045], destinations=None, timestamps=None, record_history=None, handlers=[self._Res_Co_handler_1039]), da.pat.EventPattern(da.pat.ReceivedEvent, '_Res_CoReceivedEvent_2', PatternExpr_1057, sources=[PatternExpr_1064], destinations=None, timestamps=None, record_history=None, handlers=[self._Res_Co_handler_1056]), da.pat.EventPattern(da.pat.ReceivedEvent, '_Res_CoReceivedEvent_3', PatternExpr_1132, sources=[PatternExpr_1139], destinations=None, timestamps=None, record_history=None, handlers=[self._Res_Co_handler_1131])])

    def setup(self, workers):
        self._state.workers = workers
        self._state.ongoingEvals = []
        self._state.updated_cache = {}

    def run(self):
        super()._label('_st_label_1011', block=False)
        _st_label_1011 = 0
        while (_st_label_1011 == 0):
            _st_label_1011 += 1
            if (len([p for (_, (_, _, p), (_ConstantPattern1032_,)) in self._Res_CoReceivedEvent_0 if (_ConstantPattern1032_ == 'done')]) == 1):
                _st_label_1011 += 1
            else:
                super()._label('_st_label_1011', block=True)
                _st_label_1011 -= 1

    def _Res_Co_handler_1039(self, p):
        self.output((str(self.id) + ' shutting Down'))
    _Res_Co_handler_1039._labels = None
    _Res_Co_handler_1039._notlabels = None

    def _Res_Co_handler_1056(self, req, sub_coord_id):
        self.output(('Request from Sub_Co recvd at-> ' + str(self.id)))
        self._state.ongoingEvals.append(req)
        for attr in req.res_attrs:
            if (attr in self._state.updated_cache):
                req.res_attrs[attr] = self._state.updated_cache[attr]
        workers_list = [p for p in self._state.workers]
        w_id = workers_list[random.randint(0, (len(workers_list) - 1))]
        self.output(('Sending Request from Res_Co Worker-> ' + str(w_id)))
        self._send(('evalRequestFromRes_Co', req), w_id)
    _Res_Co_handler_1056._labels = None
    _Res_Co_handler_1056._notlabels = None

    def _Res_Co_handler_1131(self, req, sub_coord_id):
        conflict_decision = None
        self.output(('Received Req for Res Conflict eval at-> ' + str(self.id)))
        if req.res_attrs_to_update:
            for attr in req.res_attrs_to_update:
                if (attr in self._state.updated_cache):
                    if (self._state.updated_cache[attr]['timestamp'] > req.timestamp):
                        conflict_decision = 'Present'
                        break
        if (conflict_decision == 'Present'):
            self.output('Resource Conflict-> Present')
            self.output(('Send Conflict msg to Sub_Co-> ' + str(sub_coord_id)))
            self._send(('conflictEvalReplyFromRes_Co', conflict_decision, req), sub_coord_id)
        else:
            self.output('No Conflict at the Resource Coordinator')
            if req.sub_attrs_to_update:
                for attr in req.res_attrs_to_update:
                    self.output('Res_Co## Updating the Cache')
                    self._state.updated_cache[attr] = req.res_attrs_to_update[attr]
            self.output('Res Co Committing updates to DB')
            self._send(('updateResAttrs', self._state.updated_cache, req.res_id), req.dbEmulator)
            self.output(('Send No Conflict msg to Sub_Co-> ' + str(sub_coord_id)))
            self._send(('conflictEvalReplyFromRes_Co', conflict_decision, req), sub_coord_id)
    _Res_Co_handler_1131._labels = None
    _Res_Co_handler_1131._notlabels = None

class Worker(da.DistProcess):

    def __init__(self, procimpl, props):
        super().__init__(procimpl, props)
        self._WorkerReceivedEvent_2 = []
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_WorkerReceivedEvent_0', PatternExpr_1270, sources=[PatternExpr_1276], destinations=None, timestamps=None, record_history=None, handlers=[self._Worker_handler_1269]), da.pat.EventPattern(da.pat.ReceivedEvent, '_WorkerReceivedEvent_1', PatternExpr_1296, sources=[PatternExpr_1302], destinations=None, timestamps=None, record_history=None, handlers=[self._Worker_handler_1295]), da.pat.EventPattern(da.pat.ReceivedEvent, '_WorkerReceivedEvent_2', PatternExpr_1672, sources=[PatternExpr_1677], destinations=None, timestamps=None, record_history=True, handlers=[]), da.pat.EventPattern(da.pat.ReceivedEvent, '_WorkerReceivedEvent_3', PatternExpr_1696, sources=[PatternExpr_1701], destinations=None, timestamps=None, record_history=None, handlers=[self._Worker_handler_1695]), da.pat.EventPattern(da.pat.ReceivedEvent, '_WorkerReceivedEvent_4', PatternExpr_1713, sources=[PatternExpr_1720], destinations=None, timestamps=None, record_history=None, handlers=[self._Worker_handler_1712]), da.pat.EventPattern(da.pat.ReceivedEvent, '_WorkerReceivedEvent_5', PatternExpr_1837, sources=[PatternExpr_1844], destinations=None, timestamps=None, record_history=None, handlers=[self._Worker_handler_1836])])

    def setup(self):
        self._state.rules = []
        self._state.sub_attr_dict = dict()
        self._state.res_attr_dict = dict()

    def run(self):
        self._state.rules = self.create_rules('policy-example.xml')
        super()._label('_st_label_1667', block=False)
        _st_label_1667 = 0
        while (_st_label_1667 == 0):
            _st_label_1667 += 1
            if (len([p for (_, (_, _, p), (_ConstantPattern1688_,)) in self._WorkerReceivedEvent_2 if (_ConstantPattern1688_ == 'done')]) == 1):
                _st_label_1667 += 1
            else:
                super()._label('_st_label_1667', block=True)
                _st_label_1667 -= 1

    def create_rules(self, filename):
        tree = ET.parse(filename)
        root = tree.getroot()
        self._state.rules = []
        for rule in root.iter('rule'):
            sub_attrs = {}
            res_attrs = {}
            action = {}
            sub_attrs_to_update = {}
            res_attrs_to_update = {}
            rule_name = rule.attrib['name']
            sc = rule.find('subjectCondition')
            sub_id = sc.attrib['position']
            for key in sc.attrib.keys():
                if (not (key == 'position')):
                    sub_attrs[key] = sc.attrib[key]
            rc = rule.find('resourceCondition')
            if ('id' in rc.attrib.keys()):
                res_id = rc.attrib['id']
            else:
                res_id = rc.attrib['type']
            for key in rc.attrib.keys():
                if ((not (key == 'id')) and (not (key == 'type'))):
                    res_attrs[key] = rc.attrib[key]
            act = rule.find('action')
            for key in act.attrib.keys():
                action = act.attrib[key]
            su = rule.find('subjectUpdate')
            if (not (su == None)):
                for key in su.attrib.keys():
                    sub_attrs_to_update[key] = su.attrib[key]
            ru = rule.find('resourceUpdate')
            if (not (ru == None)):
                for key in ru.attrib.keys():
                    res_attrs_to_update[key] = ru.attrib[key]
            temp_rule = Rule(rule_name, sub_id, res_id, action, sub_attrs, res_attrs, sub_attrs_to_update, res_attrs_to_update)
            self._state.rules.append(temp_rule)
        return self._state.rules

    def policy(self, req, sub_attrs_for_policy_eval):
        out = False
        for rule in self._state.rules:
            if ((rule.sub_id == req.sub_id) and (rule.res_id == req.res_id) and (rule.action == req.action)):
                for attr in rule.sub_attrs:
                    if ((not (attr in sub_attrs_for_policy_eval)) or (not (rule.sub_attrs[attr] == sub_attrs_for_policy_eval[attr]))):
                        out = True
                        break
                if (out == False):
                    for attr in req.res_attrs:
                        if ((not (attr in rule.res_attrs)) or (not (rule.res_attrs[attr] == req.res_attrs[attr]))):
                            out = True
                            break
                if (out == False):
                    self.output('Found a Matching Rule')
                    return (rule.sub_attrs_to_update, rule.res_attrs_to_update)
        return (None, None)

    def _Worker_handler_1269(self, sub_attr_dict, dbEmulator):
        for attr in sub_attr_dict:
            self._state.sub_attr_dict[attr] = sub_attr_dict[attr]
    _Worker_handler_1269._labels = None
    _Worker_handler_1269._notlabels = None

    def _Worker_handler_1295(self, res_attr_dict, dbEmulator):
        for attr in res_attr_dict:
            self._state.res_attr_dict[attr] = res_attr_dict[attr]
    _Worker_handler_1295._labels = None
    _Worker_handler_1295._notlabels = None

    def _Worker_handler_1695(self, p):
        self.output((str(self.id) + ' shutting Down'))
    _Worker_handler_1695._labels = None
    _Worker_handler_1695._notlabels = None

    def _Worker_handler_1712(self, req, res_coord_id):
        self.output(('Received Request from Res_Co at-> ' + str(self.id)))
        sub_attrs_for_policy_eval = {}
        for attr in req.sub_attrs:
            if (attr in req.attrs_read_from_tent):
                sub_attrs_for_policy_eval[attr] = req.attrs_read_from_tent[attr]
            elif (attr in req.attrs_read_from_cache):
                sub_attrs_for_policy_eval[attr] = req.attrs_read_from_cache[attr]
            elif ((req.sub_id in self._state.sub_attr_dict) and (attr in self._state.sub_attr_dict[req.sub_id])):
                sub_attrs_for_policy_eval[attr] = self._state.sub_attr_dict[req.sub_id][attr]
        self.output(('Value Read before Rule-- ' + str(sub_attrs_for_policy_eval)))
        req.sub_attrs_for_policy_eval = sub_attrs_for_policy_eval
        (sub_attrs_to_update, res_attrs_to_update) = self.policy(req, sub_attrs_for_policy_eval)
        req.sub_attrs_to_update = sub_attrs_to_update
        req.res_attrs_to_update = res_attrs_to_update
        self.output(('Sending Decision from Worker to Sub_Co-> ' + str(req.hashMap[req.sub_id])))
        self._send(('decisionFromWorker', req), req.hashMap[req.sub_id])
    _Worker_handler_1712._labels = None
    _Worker_handler_1712._notlabels = None

    def _Worker_handler_1836(self, sub_attr_dict, res_attr_dict, dbEmulator):
        self._state.sub_attr_dict = sub_attr_dict
        self._state.res_attr_dict = res_attr_dict
    _Worker_handler_1836._labels = None
    _Worker_handler_1836._notlabels = None

class DB_Emulator(da.DistProcess):

    def __init__(self, procimpl, props):
        super().__init__(procimpl, props)
        self._DB_EmulatorReceivedEvent_0 = []
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_DB_EmulatorReceivedEvent_0', PatternExpr_2110, sources=[PatternExpr_2115], destinations=None, timestamps=None, record_history=True, handlers=[]), da.pat.EventPattern(da.pat.ReceivedEvent, '_DB_EmulatorReceivedEvent_1', PatternExpr_2134, sources=[PatternExpr_2139], destinations=None, timestamps=None, record_history=None, handlers=[self._DB_Emulator_handler_2133]), da.pat.EventPattern(da.pat.ReceivedEvent, '_DB_EmulatorReceivedEvent_2', PatternExpr_2167, sources=[PatternExpr_2176], destinations=None, timestamps=None, record_history=None, handlers=[self._DB_Emulator_handler_2166]), da.pat.EventPattern(da.pat.ReceivedEvent, '_DB_EmulatorReceivedEvent_3', PatternExpr_2196, sources=[PatternExpr_2205], destinations=None, timestamps=None, record_history=None, handlers=[self._DB_Emulator_handler_2195]), da.pat.EventPattern(da.pat.ReceivedEvent, '_DB_EmulatorReceivedEvent_4', PatternExpr_2225, sources=[PatternExpr_2234], destinations=None, timestamps=None, record_history=None, handlers=[self._DB_Emulator_handler_2224]), da.pat.EventPattern(da.pat.ReceivedEvent, '_DB_EmulatorReceivedEvent_5', PatternExpr_2298, sources=[PatternExpr_2307], destinations=None, timestamps=None, record_history=None, handlers=[self._DB_Emulator_handler_2297])])

    def setup(self, workers, db_config_file):
        self._state.workers = workers
        self._state.db_config_file = db_config_file
        self._state.sub_attr_dict = dict()
        self._state.res_attr_dict = dict()
        self._state.minDBLatency = 0.0
        self._state.maxDBLatency = 1.0

    def run(self):
        config = configparser.ConfigParser()
        config.read(self._state.db_config_file)
        sub_section = config['Subject']
        sub_id = sub_section['sub_id'].strip()
        sub_attrs_list = sub_section['sub_attrs'].strip().split(',')
        attr_dict = {}
        if (len(sub_attrs_list) >= 1):
            for attrs in sub_attrs_list:
                attr_dict[attrs.strip().split(':')[0]] = attrs.strip().split(':')[1]
        self._state.sub_attr_dict[sub_id] = attr_dict
        for w_id in self._state.workers:
            self.output(('Sending updates to worker-> ' + str(w_id)))
            self._send(('subAttrsFromDB', self._state.sub_attr_dict), w_id)
        res_section = config['Resource']
        res_id = res_section['res_id'].strip()
        res_attrs_list = res_section['res_attrs'].strip().split(',')
        attr_dict = {}
        if ((len(res_attrs_list) >= 1) and all((v for v in res_attrs_list))):
            for attrs in res_attrs_list:
                attr_dict[attrs.strip().split(':')[0]] = attrs.strip().split(':')[1]
        self._state.res_attr_dict[res_id] = attr_dict
        for w_id in self._state.workers:
            self.output(('Sending updates to worker-> ' + str(w_id)))
            self._send(('resAttrsFromDB', self._state.res_attr_dict), w_id)
        latency_section = config['Latency']
        self._state.minDBLatency = int(latency_section['minDBLatency'])
        self._state.maxDBLatency = int(latency_section['maxDBLatency'])
        super()._label('_st_label_2105', block=False)
        _st_label_2105 = 0
        while (_st_label_2105 == 0):
            _st_label_2105 += 1
            if (len([p for (_, (_, _, p), (_ConstantPattern2126_,)) in self._DB_EmulatorReceivedEvent_0 if (_ConstantPattern2126_ == 'done')]) == 1):
                _st_label_2105 += 1
            else:
                super()._label('_st_label_2105', block=True)
                _st_label_2105 -= 1

    def _DB_Emulator_handler_2133(self, Master):
        self.output('DONE recvd at the dbEmulator')
        self.output('Dumping the DataBase')
        self.output(('Subject DataBase-> ' + str(self._state.sub_attr_dict)))
        self.output(('Resource DataBase-> ' + str(self._state.res_attr_dict)))
        self.output((str(self.id) + ' shutting Down'))
    _DB_Emulator_handler_2133._labels = None
    _DB_Emulator_handler_2133._notlabels = None

    def _DB_Emulator_handler_2166(self, sub_attrs, sub_id, w_id):
        self.output(('Sending sub_attrs to worker->' + str(w_id)))
        self._send(('subAttrsFromDB', self._state.sub_attr_dict[sub_id]), w_id)
    _DB_Emulator_handler_2166._labels = None
    _DB_Emulator_handler_2166._notlabels = None

    def _DB_Emulator_handler_2195(self, res_attrs, res_id, w_id):
        self.output(('Sending res_attrs to worker->' + str(w_id)))
        self._send(('resAttrsFromDB', self._state.res_attr_dict[res_id]), w_id)
    _DB_Emulator_handler_2195._labels = None
    _DB_Emulator_handler_2195._notlabels = None

    def _DB_Emulator_handler_2224(self, sub_attrs, sub_id, sub_coord_id):
        for attr in sub_attrs:
            self._state.sub_attr_dict[sub_id][attr] = sub_attrs[attr]
        self.output(('Recvd req to update by Sub_Co-> ' + str(sub_coord_id)))
        waittime = random.uniform(self._state.minDBLatency, self._state.maxDBLatency)
        self.output(('Latency Chosen by the DB-> ' + str(waittime)))
        time.sleep(waittime)
        for w_id in self._state.workers:
            self.output(('Sending updates to worker-> ' + str(w_id)))
            self._send(('subAttrsFromDB', self._state.sub_attr_dict), w_id)
    _DB_Emulator_handler_2224._labels = None
    _DB_Emulator_handler_2224._notlabels = None

    def _DB_Emulator_handler_2297(self, res_attrs, res_id, res_coord_id):
        for attr in res_attrs:
            self._state.res_attr_dict[res_id][attr] = res_attrs[attr]
        self.output(('Recvd req to update by res co-> ' + str(res_coord_id)))
        waittime = random.uniform(self._state.minDBLatency, self._state.maxDBLatency)
        self.output(('Latency Chosen by the DB-> ' + str(waittime)))
        time.sleep(waittime)
        for w_id in self._state.workers:
            self.output(('Sending updates to worker-> ' + str(w_id)))
            self._send(('resAttrsFromDB', self._state.res_attr_dict), w_id)
    _DB_Emulator_handler_2297._labels = None
    _DB_Emulator_handler_2297._notlabels = None

class Master(da.DistProcess):

    def __init__(self, procimpl, props):
        super().__init__(procimpl, props)
        self._MasterReceivedEvent_0 = []
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_MasterReceivedEvent_0', PatternExpr_2825, sources=[PatternExpr_2830], destinations=None, timestamps=None, record_history=True, handlers=[]), da.pat.EventPattern(da.pat.ReceivedEvent, '_MasterReceivedEvent_1', PatternExpr_2866, sources=[PatternExpr_2871], destinations=None, timestamps=None, record_history=None, handlers=[self._Master_handler_2865])])

    def setup(self, config_file_name, db_config_file):
        self._state.config_file_name = config_file_name
        self._state.db_config_file = db_config_file
        pass

    def run(self):
        config = configparser.ConfigParser()
        config.read(self._state.config_file_name)
        self.output(str(config.sections()))
        master_section = config['Master']
        num_of_workers = int(master_section['num_of_workers'])
        num_of_sub_co = int(master_section['num_of_sub_co'])
        num_of_res_co = int(master_section['num_of_res_co'])
        sub_id_section = config['sub-id-list']
        res_id_section = config['res-id-list']
        hashMap = {}
        workers = da.new(Worker, [], num=num_of_workers)
        sub_co = da.new(Sub_Co, [], num=num_of_sub_co)
        res_co = da.new(Res_Co, [workers], num=num_of_res_co)
        dbEmulator = da.new(DB_Emulator, [workers, self._state.db_config_file], num=1)
        sub_co_list = [p for p in sub_co]
        res_co_list = [p for p in res_co]
        sub_id_list = sub_id_section['sub_id_list'].strip().split(',')
        res_id_list = res_id_section['res_id_list'].strip().split(',')
        i = 0
        for sub_id in sub_id_list:
            hashMap[sub_id] = sub_co_list[i]
            i = (i + 1)
            i = (i % num_of_sub_co)
        i = 0
        for res_id in res_id_list:
            hashMap[res_id] = res_co_list[i]
            i = (i + 1)
            i = (i % num_of_res_co)
        self.output(('Mapping: ' + str(hashMap)))
        app_section = config['Application']
        num_of_requests = int(app_section['num_of_request'])
        self.output(('number of Request:' + str(num_of_requests)))
        for i in range(1, (num_of_requests + 1)):
            req = app_section[str(i)].strip().split(',')
            sub_attrs = {}
            res_attrs = {}
            for elem in req:
                elem = elem.split('=')
                if (elem[0].strip() == 'sub_id'):
                    sub_id = elem[1]
                    sub_id_list.append(sub_id)
                elif ((elem[0].strip() == 'sub_attrs') and (not (elem[1].strip() == 'None'))):
                    sub_attrs_list = elem[1].split('|')
                    for e in sub_attrs_list:
                        sub_attrs[e.split(':')[0]] = e.split(':')[1]
                elif (elem[0].strip() == 'res_id'):
                    res_id = elem[1]
                    res_id_list.append(res_id)
                elif ((elem[0].strip() == 'res_attrs') and (not (elem[1].strip() == 'None'))):
                    res_attrs_list = elem[1].split('|')
                    for e in res_attrs_list:
                        res_attrs[e.split(':')[0]] = e.split(':')[1]
                elif (elem[0].strip() == 'action'):
                    action = elem[1]
            req = Request(sub_id, res_id, hashMap, sub_attrs, res_attrs, action)
            app = da.new(Application, [hashMap, sub_co, res_co, self.id, dbEmulator, req], num=1)
            self.output(str(app))
            da.start(app)
        da.start((((sub_co | res_co) | workers) | dbEmulator))
        super()._label('_st_label_2820', block=False)
        _st_label_2820 = 0
        while (_st_label_2820 == 0):
            _st_label_2820 += 1
            if (len([p for (_, (_, _, p), (_ConstantPattern2841_,)) in self._MasterReceivedEvent_0 if (_ConstantPattern2841_ == 'okay')]) == num_of_requests):
                _st_label_2820 += 1
            else:
                super()._label('_st_label_2820', block=True)
                _st_label_2820 -= 1
        self._send(('done',), (((sub_co | res_co) | workers) | dbEmulator))
        self.output((str(self.id) + ' shutting Down'))

    def _Master_handler_2865(self, p):
        self._send(('done',), p)
    _Master_handler_2865._labels = None
    _Master_handler_2865._notlabels = None

class _NodeMain(da.DistProcess):

    def run(self):
        config_file_name = (str(sys.argv[1]) if (len(sys.argv) > 1) else 'basic.config')
        db_config_file = (str(sys.argv[2]) if (len(sys.argv) > 2) else 'dbconfig.config')
        log_file_name = (config_file_name.strip().split('.')[0] + '.log')
        self.output(log_file_name)
        logging.getLogger('').handlers = []
        logging.basicConfig(filename=log_file_name, filemode='w', level=logging.INFO)
        master = da.new(Master, [config_file_name, db_config_file], num=1)
        da.start(master)
