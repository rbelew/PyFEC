## pyfec.py
## Python analysis of FEC data as provided by CIR via Kaggle
## October, 2012
##
## v. 0.1
## Rik Belew (rik@electronicArtifacts.com)
## 27 Oct 12

import _mysql

import csv
import cPickle as pickle
import re
import random
import math

DataDir = '/Data/corpora/kaggle_CIR/'

## Database variables & routines

SchemaTableNames = [] # list of table names
SchemaColumns = {} # tblName -> [columnNameList]

TableLoadOrder = ('fec_itemized_cand_2012', 
				'fec_itemized_comm_2012', 
				'fec_itemized_ccl_2012', 
				'fec_itemized_indiv_2012', 
				'fec_itemized_pas_2012',
				'fec_itemized_oth_2012'
				)

DBSpecTbl = { \
			# rootName -> (idAttr, [ (fkeyAttr,fObj,foAttr,add?) ], [keepAttr] )
			"cand": ('candidate_id',[],["candidate_name", "party", "candidate_state", "candidate_office", \
										"current_district", "incum_challenger_openseat", "candidate_status", \
										"principle_campaign_committee_id", "city", "state", "zip", "fec_election_year"]),
			"comm": ('committee_id',[],["committee_name", "treasurers_name", "city", "state", "zip", "nicar_election_year"]),

			"ccl": ("linkage_id",[("candidate_id","cand","candidate_id",False), ("committee_id","comm","committee_id",False)], \
                     ["fec_election_year"]),

			"indiv": ("transaction_id", [("filer_id","comm","committee_id",False),("contributor_name","contrib","name",True)], \
                     ["transaction_type", "nicar_date","amount","other_id", "nicar_election_year"]),

			"oth":   ("transaction_id", [("filer_id","comm","committee_id",False),("contributor_name","contrib","name",True)], \
                     ["transaction_type", "nicar_date","amount","other_id", "nicar_election_year"]),

			"pas":   ("transaction_id", [("filer_id","comm","committee_id",False),("candidate_id","cand","candidate_id",False)],
                     ["transaction_type", "nicar_date","amount","other_id", "nicar_election_year"])
			}

class Cand():	
	pass

class Ccl():	
	pass

class Comm():	
	pass

class Indiv():	
	pass

class Oth():	
	pass

class Pas():	
	pass

class Contrib():
	pass

CandTbl = {}
CclTbl = {}
CommTbl = {}
IndivTbl = {}
OthTbl = {}
PasTbl = {}

ContribTbl = {}

AllTbls = {}
AllTblNames = ['cand', 'comm', 'contrib', 'ccl', 'indiv', 'pas', 'oth']

class DBConn():
	def __init__(self,dbName=''):
		self.currDB = _mysql.connect(host="localhost",user="root", passwd="sql4monk", db=dbName)

	def get_tableNames(self):
		self.currDB.query('show tables;')
		r=self.currDB.store_result()

		tblNames = []
		for ri in range(r.num_rows()):
			f = r.fetch_row()
			# (('fec_itemized_cand_2012',),)
			tname = f[0][0]
			tblNames.append(tname)

		return tblNames

	def get_colNames(self,tblName):

		self.currDB.query("select column_name from information_schema.columns where table_name = '%s';" % tblName)
		r=self.currDB.store_result()

		colNames = []
		for ri in range(r.num_rows()):
			f = r.fetch_row()
			# (('fec_itemized_cand_2012',),)
			cname = f[0][0]
			colNames.append(cname)
			
		return colNames
	
def reconn(dbName):
	db = DBConn(dbName)
	tblNames = db.get_tableNames()
	for tname in tblNames:
		print '*',tname
		for cname in db.get_colNames(tname):
			print cname
	
def getRootName(tname):
	# 'fec_itemized_cand_2012' -> 'cand'
	return tname[13:-5]

def testCandID(rid,row,colNames):

	idoff = rid[0]
	idstate = rid[2:4]
	iddist = rid[4:6]
	off = row[colNames.index("candidate_office")]
	state = row[colNames.index("state")]
	dist = row[colNames.index("current_district")]
	
	rv = idoff==off and idstate==state and iddist==dist
#	if not rv:
#		print 'odd CandID?!',rid,off,state,dist
	return rv

def bldPyDict(db,tname,trn):
	'return dictionary of _id -> tname objects'
	
	db.currDB.query("select * from %s;" % tname)

	r=db.currDB.store_result()

	nrows = r.num_rows()
	print 'bldPyDict: Table %s has %d rows' % (trn,nrows)
	
	colNames = SchemaColumns[trn]
	# ASSUME first attribute is UID
	assert colNames[0].endswith('_id'), 'ASSUME first attribute is UID?!'
	
	logicSpec = DBSpecTbl[trn]
	# (idAttr, [ (fkeyAttr,fObj,foAttr,add?) ], [keepAttr] )
	idAttr, fkeyList, keepList = logicSpec
	
	newTbl = {}
	allForeignRefTbl = {}
	nRptErr = 5
	nmiss=0
	nadd=0
	ndup=0
	nnull=0
	nnullfkey=0
	ncull=0
	ncomm=0
	ncand=0
	
	noddCand=0
	
	# NB: silently ignore non-2012 entries
	if trn=='comm' or trn=='indiv' or trn=='oth' or trn=='pas':
		yrAttrib = "nicar_election_year"
	else:
		yrAttrib = "fec_election_year"

	for ri in range(nrows):
		row = r.fetch_row()[0] # ?? why first element of row?
		
		assert len(row)==len(colNames), '#attribues != #values?!'
		

		# NB: silently ignore non-2012 entries
		idx = colNames.index(yrAttrib)
		eyear = row[ idx ]
		if eyear != '2012':
			continue
		
		idx = colNames.index(idAttr)
		rid = row[ idx ]
		rid = rid.strip()
		if rid=='':
			if nnull < nRptErr:
				print 'bldPyDict: null id?! %s: currRow=%d' % (trn,ri)
			nnull += 1
			continue
		
		if rid in newTbl:
			if ndup < nRptErr:
				print 'bldPyDict: dup id?! %s:%s currRow=%d prevRow=%d' % (trn,rid,ri,newTbl[rid].rowNum)
			ndup += 1
			continue
		
		if trn=='cand':
			match = testCandID(rid,row,colNames)
			if not match:
				noddCand += 1
				
		# NB: avoid double counting PAS contributions included in other
		# ASSUME pas loaded before oth
		if (trn=='oth' and rid in AllTbls['pas']):
			if ndup < nRptErr:
				print 'bldPyDict: oth dup with pas id?! %s:%s currRow=%d' % (trn,rid,ri)
			ndup += 1
			continue 
		newObj = None # to kill pydev syntax errors
		
		currfo0 = None
		currfo1 = None
		currfobj1 = None # actual object retrieved
	
		# ASSUME 2 foreign keys ==> RELATION between foreign objects
		# create newObj of type trn with id and any additional attributes
		# add its ID to lists in both foreign objects
		
		if len(fkeyList)==2:
			fkTbl = {}
			fka0,fo0,foa0,addP0 = fkeyList[0]
			fka1,fo1,foa1,addP1 = fkeyList[1]
				
			currForeignTbl0 = AllTbls[fo0]
			currForeignTbl1 = AllTbls[fo1]
				
			idx = colNames.index(fka0)
			fkey0 = row[ idx ]
			fkey0 = fkey0.strip()
			if fkey0=='':
				if nnullfkey < nRptErr:
					print 'bldPyDict: null fkey0?! %s:%s currRow=%d' % (trn,rid,ri)
				nnullfkey += 1
				continue

			if fkey0 in currForeignTbl0:
				# cull dormant committees!
				if fo0=='comm' and fkey0 in Comm2CullTbl:
					ncull += 1
					continue
				currfo0 = fkey0
			elif addP0:
				if nadd < nRptErr:
					print 'bldPyDict: adding new %s:%s from %s:%s' % (fo0,fkey0,trn,rid)
				nadd += 1
				newFObj = eval('%s()' % (fo0.capitalize()))
				setattr(newFObj,"id",fkey0)
				currForeignTbl0[fkey0] = newFObj
				currfo0 = fkey0
			else:
				if nadd < nRptErr:
					print 'bldPyDict: missing fkey0 %s:%s from %s:%s' % (fo0,fkey0,trn,rid)
				fobj0 = None
				nmiss += 1
				
			# NB: for indiv and other, prefer other_id if it exists over free-text contrib name
			# 2do: what about pas with other_id?!
			
			if trn=='indiv' or trn=='oth':
				idx = colNames.index('other_id')
				fkey1 = row[ idx ]
				fkey1 = fkey1.strip()
				if fkey1!='':
					if fkey1.startswith('C'):
						if fkey1 in AllTbls['comm']:
							currfo1 = fkey1
							currfobj1 = AllTbls['comm'][currfo1]
							ncomm += 1
					elif fkey1.startswith('P') or fkey1.startswith('S') or fkey1.startswith('H'):
						if fkey1 in AllTbls['cand']:
							currfo1 = fkey1
							currfobj1 = AllTbls['cand'][currfo1]
							ncand += 1
					else:
						print 'bldPyDict: odd other_id?! %s in %s:%s' % (fkey1,trn,rid)
						
			# if trn=='indiv' or trn=='oth' and haven't found valid other_id, proceed normally
			if not currfo1:

				idx = colNames.index(fka1)
				fkey1 = row[ idx ]
				fkey1 = fkey1.strip()
				if fkey1=='':
					if nnullfkey < nRptErr:
						print 'bldPyDict: null fkey1?! %s:%s currRow=%d' % (trn,rid,ri)
					nnullfkey += 1
					continue
	
				if fkey1 in currForeignTbl1:			
					# cull dormant committees!
					if fo1=='comm' and fkey1 in Comm2CullTbl:
						ncull += 1
						continue
					currfo1 = fkey1
					currfobj1 = currForeignTbl1[currfo1]
	
				elif addP1:
					if nadd < nRptErr:
						print 'bldPyDict: adding new %s:%s from %s:%s' % (fo1,fkey1,trn,rid)
					nadd += 1
					newFObj = eval('%s()' % (fo1.capitalize()))
					setattr(newFObj,"id",fkey1)
					currForeignTbl1[fkey1] = newFObj
					currfo1 = fkey1
					currfobj1 = currForeignTbl1[currfo1]
				else:
					if nmiss < nRptErr:
						print 'bldPyDict: mising fkey %s:%s from %s:%s' % (fo1,fkey1,trn,rid)
					nmiss += 1
				
		## error checking will have caused some to 'continue' before this
		## those with foreign keys have additional criteria
		
		if len(fkeyList)==2 and not(currfo0 and currfo1):
			continue
		
		exec('newObj = %s()' % (trn.capitalize()))
		setattr(newObj,"id",rid)
		setattr(newObj,'rowNum',ri)
		
		if len(keepList)>0:
			for ka in keepList:
				idx = colNames.index(ka)
				val = row[ idx ]
				setattr(newObj,ka,val)
			
		if currfo0 and currfo1:
			
			setattr(newObj,fo0,currfo0)
			setattr(newObj,fo1,currfo1)

			fobj0 = currForeignTbl0[currfo0]
			currList = getattr(fobj0,trn,[])
			currList.append(rid)
			setattr(fobj0,trn,currList)

			# NB, currfobj1 bound above, as might be of different types
			currList = getattr(currfobj1,trn,[])
			currList.append(rid)
			setattr(currfobj1,trn,currList)
				
		newTbl[rid] = newObj
		
	print 'bldPyDict: done. N_%s=%d NDup=%d NMiss=%d NAdd=%d NCull=%d NNull=%d NNullFK=%d NCand' % (trn,len(newTbl),ndup,nmiss,nadd,ncull,nnull,nnullfkey)
	if trn=='cand':
		print '\tNoddCand=%d' % (noddCand)
	return newTbl		
			
Comm2CullTbl = {}
def  parseAll(dbName,commCullFile=None):
	db = DBConn(dbName)

	global SchemaTableNames
	SchemaTableNames = db.get_tableNames()
	
	global SchemaColumns
	for tname in SchemaTableNames:
		rootName = getRootName(tname)
		# NB rootNames are keys, tname needed by db
		SchemaColumns[rootName] = db.get_colNames(tname)

	global AllTbls
	AllTbls['contrib'] = ContribTbl
	
	if commCullFile:
		dormantFile = DataDir+commCullFile
		print ('parseAll: loading dormant committees from %s...' % dormantFile),
		global Comm2CullTbl
		Comm2CullTbl = {}
		csvDictReader = csv.DictReader(open(dormantFile,"r"),delimiter="\t")
	
		for i,entry in enumerate(csvDictReader):
			cid = entry['CommID']
			cname =	entry['Comm.Name']
			Comm2CullTbl[cid] = cname
			Comm2CullTbl[cid]
			
		print 'done. NDormant=%d' % (len(Comm2CullTbl))
	
	for tname in TableLoadOrder:
		
		rootName = getRootName(tname)
		
		currTbl = bldPyDict(db,tname,rootName)
		
		AllTbls[rootName] = currTbl
		
	print 'done.'
	for tblName in AllTbls.keys():
		tbl = AllTbls[tblName]
		pklFile = DataDir+'%sTbl.pkl' % tblName
		print 'Pickling %d to %s...' % (len(tbl),pklFile)
		# can't use cPickle, cuz of my special class objects (:
		pstr = open(pklFile, 'w')
		pickle.Pickler(pstr).dump(tbl)
		pstr.close()
		tbl = {}
		del AllTbls[tblName]
	
def loadAllTbls():
	global AllTbls
	AllTbls = {}
	for tblName in AllTblNames:
		pklFile = DataDir+'%sTbl.pkl' % tblName
		print ('Loading from %s...' % pklFile),
		pstr = open(pklFile)
		tbl = pickle.Unpickler(pstr).load()
		AllTbls[tblName] = tbl
		pstr.close()
		print ' done. N=%d' % len(tbl)
		
	print 'loadAllTbls: done.'

def loadOneTbl(tname):
	pklFile = DataDir+'%sTbl.pkl' % tname
	print ('Loading from %s...' % pklFile),
	pstr = open(pklFile)
	tbl = pickle.Unpickler(pstr).load()
	pstr.close()
	print ' done. N=%d' % len(tbl)
	return tbl

## Analysis routines


def analCand():
	'''handle against Type24 payments with commid_
	   drop NTopComm threshold
	   maintain committee cumms'''
	
	global AllTbls
	
	# 2do: collapse all these loadOneTbl() paras to function, but with global consequence!?
	
	if 'cand' in AllTbls:
		candTbl = AllTbls['cand']
	else:
		candTbl = loadOneTbl('cand')
		AllTbls['cand'] = candTbl
		
	if 'pas' in AllTbls:
		pasTbl = AllTbls['pas']
	else:
		pasTbl = loadOneTbl('pas')
		AllTbls['pas'] = pasTbl

	outs = open(DataDir+'candSumm.csv','w')
	outs.write('CandID,Name,Party,State,Office,District,Comm1,NComm,Tot,NContrib,NegTot,NNeg\n')
	outs2 = open(DataDir+'cand2comm.csv','w')
	outs2.write('CandID,CommID,Amt\n')

	nzero = 0
	ncand = 0
	nedge=0
	
	cummCommTbl = {}

#	negAmtTbl = {} # typeCode -> Total
	for cid,cand in candTbl.items():
		
		tot = 0
		antitot = 0
		commTbl = {}
		ncontrib = 0
		nanti=0
		comm1 = cand.principle_campaign_committee_id
		
		if not ('pas' in dir(cand) and len(cand.pas)>0):
			nzero += 1
			continue

		
		ncand += 1
		ncontrib = len(cand.pas)
		for contrib in cand.pas:
			# ASSUME it's there
			pas = pasTbl[contrib]
			ptype = pas.transaction_type
			amt = int(round(float(pas.amount)))
			commID = pas.comm
			negAmtFnd=False
			
			# http://influenceexplorer.com/about/methodology/campaign_finance
			# discovered 24 Oct 12 !
			# 
			# In deciding what contributions to include, we attempt to follow the methodology of CRP and NIMSP:
			# 
			#     For federal contributions from individuals, we only count contributions with an FEC transaction type of 10, 11, 15, 15e, 15j or 22y.
			#     For federal contributions from organizations, we only count contributions with an FEC transaction type of 24k, 24r or 24z.
			#     We ignore all contributions with a category code of starting with 'Z', except for codes beginning with 'Z90' (candidate self-contributions), which are included.

			# treat neg amounts as anti
			if amt < 0:
				amt = - amt
				negAmtFnd=True
					
			# NB: "against" transaction types
			if negAmtFnd or ptype == '24A' or ptype == '24N':
				commID += '_'
				antitot += amt
				nanti += 1
			else:
				tot += amt
			
			if commID in commTbl:
				commTbl[commID] += amt
			else:
				commTbl[commID] = amt
					
		commKeys = commTbl.keys()

		cname = cand.candidate_name
		cname = cname.replace('"',"''")
		outs.write('%s,"%s",%s,%s,%s,%s,%s,%d,%d,%d,%d,%d\n' % \
				(cid,cname, cand.party, cand.candidate_state, \
					cand.candidate_office, cand.current_district, comm1, \
					len(commTbl),tot,ncontrib, antitot,nanti))
		
		for ck in commKeys:
			nedge+=1
			outs2.write('%s,%s,%d\n' % (cid,ck,commTbl[ck]))
			if ck in cummCommTbl:
				cummCommTbl[ck] += commTbl[ck]
			else:
				cummCommTbl[ck] = commTbl[ck]

	outs.close()
	outs2.close()
	print 'analCand: NCand=%d Nzero=%d NEdge=%d' % (ncand,nzero,nedge)
	# see /Data/corpora/kaggle_CIR/logs/analCand2_121024.log
#	print '# PType Amt'
#	for ptype,amt in negAmtTbl.items():
#		print ptype,amt

	# Create committee summary, combining pos and anti contrib
	combineCommTbl = {}
	for k,v in cummCommTbl.items():
		if k.endswith('_'):
			basek = k[:-1]
			anti = 1
		else:
			basek = k
			anti = 0
		currSumms = combineCommTbl.get(basek, [0,0] )
		currSumms[anti] = cummCommTbl[k]
		combineCommTbl[basek] = currSumms
			
	
	outs3 = open(DataDir+'commCandTot.csv','w')
	outs3.write('CommID,Pos,Neg,Tot\n')
	commKeys = combineCommTbl.keys()
	commKeys.sort()
	for commID in commKeys:
		pos = combineCommTbl[commID][0]
		neg = combineCommTbl[commID][1]
		outs3.write('%s,%d,%d,%d\n' % (commID,pos,neg,pos+neg) )
	outs3.close()
		

	
SmallGivePrefix = 'otherStrata'

def analTopContrib(ntop=300):
	global AllTbls
	if 'contrib' in AllTbls:
		contribTbl = AllTbls['contrib']
	else:
		contribTbl = loadOneTbl('contrib')
		AllTbls['contrib'] = contribTbl
		
	if 'indiv' in AllTbls:
		indivTbl = AllTbls['indiv']
	else:
		indivTbl = loadOneTbl('indiv')
		AllTbls['indiv'] = indivTbl
		
	if 'oth' in AllTbls:
		othTbl = AllTbls['oth']
	else:
		othTbl = loadOneTbl('oth')
		AllTbls['oth'] = othTbl
		
	contribList = []
	allContrib = 0
	for cid,contrib in contribTbl.items():
		
		itot = 0
		nindiv=0
		otot = 0
		nother=0
		if 'indiv' in dir(contrib):
			nindiv = len(contrib.indiv)
			for contrib2 in contrib.indiv:
				indiv = indivTbl[contrib2]
				amt = int(round(float(indiv.amount)))
				if amt<0:
					amt = -amt
				itot += amt
		if 'oth' in dir(contrib):
			nother= len(contrib.oth)
			for contrib2 in contrib.oth:
				oth = othTbl[contrib2]
				amt = int(round(float(oth.amount)))
				if amt<0:
					amt = -amt
				otot += amt
		bothTot = itot+otot	
		allContrib += (itot+otot)
		contribList.append( (cid,bothTot,nindiv,itot,nother,otot) )
		
	ncontrib = len(contribTbl)
	assert ncontrib == len(contribList), 'analTopContrib: dropped contrib?!'
	
	print 'analTopContrib: allContrib = %e NContrib=%d ' % (allContrib,len(contribList))
	
	print 'analTopContrib: sorting contribList...' 	
	contribList.sort(key=(lambda x: x[1]),reverse=True)
	
	contribRpt = DataDir+'topContrib.csv'
	print 'writing to ',contribRpt
	outs = open(contribRpt,'w')
	outs.write('# Contrib,NIndiv,ITot,NOther,OTot,BothTot\n')
	for ci, cinfo in enumerate(contribList[:ntop]):
		cid,bothtot,nindiv,itot,nother,otot = cinfo
		outs.write('"%s",%d,%d,%d,%d,%d\n' % (cid,nindiv,itot,nother,otot,bothtot))
	outs.close()
		

	
def analContrib(computeStrata=False,outOtherComm=False,filterContrib=False):
	'''v2: ASSUME contrib4comm_top and cand2comm lists already build
			form cohorts of BigSpenders and then strata of others into
			FracPercent bands
			create pseudo contribNames for all but biggest spenders
			and produce contrib,committee,amt records for graph
	'''

	c4cTbl = {}
	c2cTbl = {}
	if filterContrib:
		contrib4commf = '/Data/corpora/kaggle_CIR/analData/contrib4comm_top.csv'
		csvDictReader = csv.DictReader(open(contrib4commf,"r"))
	
		
		for i,entry in enumerate(csvDictReader):
			# CommID,CommName,Tot
			cid = entry['CommID']
			c4cTbl[cid] = int(entry['Tot'])
			
		cand2commf = '/Data/corpora/kaggle_CIR/analData/cand2comm.csv'
		csvDictReader = csv.DictReader(open(cand2commf,"r"))
	
		for i,entry in enumerate(csvDictReader):
			# CandID,CommID,Amt
			# Source,Target,Weight
	
			cid = entry['Target']
			c2cTbl[cid] = (cid,entry['Amt'])

	global AllTbls
	if 'contrib' in AllTbls:
		contribTbl = AllTbls['contrib']
	else:
		contribTbl = loadOneTbl('contrib')
		AllTbls['contrib'] = contribTbl
		
	if 'indiv' in AllTbls:
		indivTbl = AllTbls['indiv']
	else:
		indivTbl = loadOneTbl('indiv')
		AllTbls['indiv'] = indivTbl
		
	if 'oth' in AllTbls:
		othTbl = AllTbls['oth']
	else:
		othTbl = loadOneTbl('oth')
		AllTbls['oth'] = othTbl
		
	if 'comm' in AllTbls:
		commTbl = AllTbls['comm']
	else:
		commTbl = loadOneTbl('comm')
		AllTbls['comm'] = commTbl

	NStrata = 10
	# 23 Oct 12
	StrataThreshVec = [1000, 2185, 3500, 6000, 10900, 25000, 48700, 80100, 144700]

	# V3: provide for pre-computed StrataThreshVec
	if computeStrata:
		contribList = []
		allContrib = 0
		for cid,contrib in contribTbl.items():
			
			bothTot = 0
			if 'indiv' in dir(contrib):
				for contrib2 in contrib.indiv:
					# ASSUME it's there
					indiv = indivTbl[contrib2]
					amt = int(round(float(indiv.amount)))
					# NB: what are negative numbers about?! 
					# but they only total $1.5e6; ignore
					if amt<0:
						amt = -amt
					bothTot += amt
			if 'oth' in dir(contrib):
				for contrib2 in contrib.oth:
					# ASSUME it's there
					oth = othTbl[contrib2]
					amt = int(round(float(oth.amount)))
					# NB: what are negative numbers about?! 
					# but they only total $1.5e6; ignore
					if amt<0:
						amt = -amt
					bothTot += amt
					
			allContrib += bothTot
			contribList.append( (cid,bothTot) )
			
		ncontrib = len(contribTbl)
		assert ncontrib == len(contribList), 'analContrib2: dropped contrib?!'
		
		print 'analContrib2: allContrib = %e NContrib=%d ' % (allContrib,len(contribList))
		
		print 'analContrib2: sorting contribList...' 	
		contribList.sort(key=(lambda x: x[1]))
		
		runSum = 0
		stratSum = 0
		si = 0
		stratThreshFrac = allContrib / float(NStrata)

		strataVec = [0 for i in range(NStrata)]
		
		# computed 18 Oct 12
		
		zeroVal = True
		print '# ci,amt,stratSum,runSum'
		for ci, cinfo in enumerate(contribList):
			cid,amt = cinfo
			if zeroVal and amt < 1:
				continue
			elif zeroVal:
				print 'first %d zero!' % ci
				zeroVal = False
				
			stratSum += amt
			runSum += amt
			if stratSum >= stratThreshFrac:
				strataVec[si] = amt
				print '%d,%d,%d,%d' % (ci,amt,stratSum,runSum)		
				stratSum = 0
				si += 1 
				
		if strataVec != StrataThreshVec:
			print 'analContrib2: new strataVec != StrataThreshVec?!\n\tnew=%s\n\tcached=%s' % (strataVec,StrataThreshVec)


	def getStrataIdx(samt,svec=StrataThreshVec):
		'''return first i s.t. svec[i] > amt
		last category of BigSpenders > svec[-1] return -1
		'''
		for i,amt in enumerate(svec):
			if samt < amt:
				return i
		return -1

	TestVec = [10,1500,2500,5001,7500,10000,20000,40000,100000,1e6]
	# 23 Oct 12
	TestStrataVec = [0, 1, 2, 3, 4, 4, 5, 6, 8, -1]
	
	assert TestStrataVec == [getStrataIdx(amt) for amt in TestVec]
				
	## End of strata code
	
	contribRpt = DataDir+'contribSumm.csv'
	print 'writing to ',contribRpt
	outs = open(contribRpt,'w')
	outs.write('# Contrib,NIndiv,ITot,NOther,OTot\n')
	
	print 'analContrib2: Aggregating across contributions'
	outStrataTbl = {}
	# Accumulate across committees
	for cid,contrib in contribTbl.items():
		nicontrib = 0
		itot=0
		nocontrib = 0
		otot=0
		con4comTbl = {}
		if 'indiv' in dir(contrib):
			nicontrib = len(contrib.indiv)
			for contrib in contrib.indiv:
				indiv = indivTbl[contrib]
				amt = int(round(float(indiv.amount)))
				commID = indiv.comm
				# NB: only output records to 'domoinant' committees wrt/ contrib4comm or comm2cand 
				if filterContrib and not(commID in c4cTbl or commID in c2cTbl):
					if outOtherComm:
						commID = 'OtherComm'
					else:
						continue
		
				# NB: "against" transaction types
				if indiv.transaction_type == '24A' or indiv.transaction_type == '24N':
					commID = commID+'_'
				if amt<0:
					amt = -amt
				itot += amt
				if commID in con4comTbl:
					con4comTbl[commID] += amt
				else:
					con4comTbl[commID] = amt
				
		if 'oth' in dir(contrib):
			nocontrib = len(contrib.oth)
			for contrib in contrib.oth:
				oth = othTbl[contrib]
				amt = int(round(float(oth.amount)))
				commID = oth.comm			
				# NB: only output records to 'domoinant' committees wrt/ contrib4comm or comm2cand 
				if filterContrib and not(commID in c4cTbl or commID in c2cTbl):
					if outOtherComm:
						commID = 'OtherComm'
					else:
						continue
				# NB: "against" transaction types
				if oth.transaction_type == '24A' or oth.transaction_type == '24N':
					commID = commID+'_'
				if amt<0:
					amt = -amt
				otot += amt
				if commID in con4comTbl:
					con4comTbl[commID] += amt
				else:
					con4comTbl[commID] = amt

		outs.write('"%s",%d,%d,%d,%d\n' % (cid,nicontrib,itot,nocontrib,otot))

		totContrib = itot + otot
		strataIdx = getStrataIdx(totContrib)
		outContrib = None
		if strataIdx == -1:
			outContrib = cid
			# print outContrib,totContrib,len(con4comTbl)
		else:
			outContrib = '%s_%s' % (SmallGivePrefix,strataIdx)
			
		if outContrib not in outStrataTbl:
			outStrataTbl[outContrib] = {}
			
		for commID in con4comTbl.keys():
			amt = con4comTbl[commID]
			if commID in outStrataTbl[outContrib]:
				outStrataTbl[outContrib][commID] += amt
			else:
				outStrataTbl[outContrib][commID] = amt	
				
		
	outs.close()
	
	contrib2commf = DataDir+'contrib2comm.csv'
	print 'analContrib2: outputing %d contributors to %s' %(len(outStrataTbl),contrib2commf)
	nedge=0
	outs = open(contrib2commf,'w')
	outs.write('ContribID,CommID,Amt\n')

	for contrib,commTbl in outStrataTbl.items():
		for commID in commTbl.keys():
			amt = outStrataTbl[contrib][commID]
			outs.write('"%s",%s,%d\n' % (contrib,commID,amt))
			nedge += 1

	outs.close()
	print 'analContrib2 done. NEdge=%d' % (nedge)
	
def analComm():
	
	global AllTbls
	if 'comm' in AllTbls:
		commTbl = AllTbls['comm']
	else:
		commTbl = loadOneTbl('comm')
		AllTbls['comm'] = commTbl

	if 'indiv' in AllTbls:
		indivTbl = AllTbls['indiv']
	else:
		indivTbl = loadOneTbl('indiv')
		AllTbls['indiv'] = indivTbl
		
	if 'oth' in AllTbls:
		othTbl = AllTbls['oth']
	else:
		othTbl = loadOneTbl('oth')
		AllTbls['oth'] = othTbl

	if 'pas' in AllTbls:
		pasTbl = AllTbls['pas']
	else:
		pasTbl = loadOneTbl('pas')
		AllTbls['pas'] = pasTbl

	nindiv = 0
	noth = 0
	npas = 0
	nzero=0
	ncomm=0
	nc2cedge=0
	outs = open(DataDir+'commSumm.csv','w')
	
	# HACK: bars used because commas in committee names(:
	outs.write('CID,Name,NIndiv,ISum,NOther,OSum,NPas,PSum\n')
	for cid,comm in commTbl.items():
		indivLen=0
		othLen=0
		pasLen=0
		isum = 0			
		osum = 0
		psum = 0

		try:
			if 'indiv' in dir(comm):
				indivLen=len(comm.indiv)
				nindiv += 1
				for indivID in comm.indiv:
					indiv = indivTbl[indivID]
					isum += float(indiv.amount)
			if 'oth' in dir(comm):
				othLen=len(comm.oth)
				noth += 1
				for othID in comm.oth:
					oth = othTbl[othID]
					osum += float(oth.amount)
				nc2cedge += othLen
			if 'pas' in dir(comm):
				pasLen=len(comm.pas)
				npas += 1
				for pasID in comm.pas:
					pas = pasTbl[pasID]
					psum += float(pas.amount)
					
			# HACK: bars used because commans in committee names(:
			if indivLen+othLen+pasLen==0:
				nzero += 1
			else:
				ncomm += 1
				outs.write('%s,"%s",%d,%d,%d,%d,%d,%d\n' % \
						(cid,comm.committee_name,indivLen,int(round(isum)),othLen,int(round(osum)),pasLen,int(round(psum))))
			
		except Exception,e:
			print 'Err? cid=%s name=%s e=%s' % (cid,comm.committee_name,e)
			
	outs.close()
	print 'analComm: NComm=%d nzero=%d NC2CEdge=%d' % (ncomm,nzero,nc2cedge)
		
def bldRaceTbl():
	'raceID -> [(candID,state,party,ico)]'
	
	candTbl = loadOneTbl('cand')
	raceTbl = {}
	for cid,cand in candTbl.items():
		party = cand.party.strip()
		state = cand.candidate_state.strip()
		office = cand.candidate_office.strip()
		district = cand.current_district.strip()
		# raceID = '_'.join([state,office,district])
		raceID = '%s_%s%s' % (office,state,district)
		ico = cand.incum_challenger_openseat
		if raceID in raceTbl:
			raceTbl[raceID].append( (cid,state,party,ico) )
		else:
			raceTbl[raceID] = [ (cid,state,party,ico) ]
			
	print '# RaceTbl\nRaceID,NCand'
	for raceID in raceTbl.keys():
#		if len(raceTbl[raceID]) != 2:
#			print 'odd race ~2?!',raceID,raceTbl[raceID]
#		else:
#			parties = [cinfo[1] for cinfo in raceTbl[raceID]  ]
#			parties = parties.sort()
#			if parties != ['DEM','REP']:
#				print 'odd race D/R?!',raceID,raceTbl[raceID]
#			else:
#				nnorm += 1
		print '%s,%d' % (raceID,len(raceTbl[raceID]))
		
#	print 'bldRaceTbl: NRace=%d NNorm=%d' % (len(raceTbl),nnorm)		
	print 'bldRaceTbl: NRace=%d' % (len(raceTbl))
	return raceTbl

			
	
def bldGraph(contribTopCommFile,candTopCommFile,candComm1File):
	'''create nodes for all contrib and cand from contrib2comm and cand2comm edge sets, resp
		ASSUME subsetComm: use provided files as filters: committees NOT in ANY of
		these sets are dropped
		include comm2comm other links only among this set
		fold commID and commID_ nodes together but add "anti" edge type attribute 
		collect displayable node attributes
		 
	'''
	
	## Load tables for committees to be kept
	nodeTbl = {} # id -> [type, attribList, edgeList]

	keepCommTbl = {}
	for fi,inf in enumerate([contribTopCommFile,candTopCommFile,candComm1File]):
		csvDictReader = csv.DictReader(open(DataDir+'analData/'+inf,"r"))
		for i,entry in enumerate(csvDictReader):
			commID = entry['CommID']
			if commID not in keepCommTbl:
				keepCommTbl[commID] = fi
			nodeTbl[commID] = ['comm',[] ]
			
	ndropEdge = 0
	dropComm2comm = 0
	
	## build graph associated with contrib2comm
	csvDictReader = csv.DictReader(open(DataDir+'analData/'+'contrib2comm.csv',"r"))
	for i,entry in enumerate(csvDictReader):
		# ContribID,CommID,Amt
		contribID = entry['ContribID']
		# comment lines
		if contribID.startswith('# '):
			continue
				
		anti = 0
		commID = entry['CommID']
		if commID.endswith('_'):
			anti = 1
			commID = commID[:-1]
			
		if commID not in keepCommTbl:
			ndropEdge += 1
			continue

		amt = int(entry['Amt'])
	
		if contribID not in nodeTbl:
			nodeTbl[contribID] = ['contrib',[] ]
			
		nodeTbl[contribID][1].append( (commID,amt,anti) )
		
	## build graph associated with cand2comm
	csvDictReader = csv.DictReader(open(DataDir+'analData/'+'cand2comm.csv',"r"))
	for i,entry in enumerate(csvDictReader):
		# CandID,CommID,Amt
		candID = entry['CandID']
		# comment lines
		if candID.startswith('# '):
			continue
		
		anti = 0
		commID = entry['CommID']
		if commID.endswith('_'):
			anti = 1
			commID = commID[:-1]

		if commID not in keepCommTbl:
			ndropEdge += 1
			continue
		
		amt = int(entry['Amt'])
	
		if candID not in nodeTbl:
			nodeTbl[candID] = ['cand',[], ]

		# NB: edge directed FROM comm TO cand					
		nodeTbl[commID][1].append( (candID,amt,anti))

	## Add comm2comm edges
	
	global AllTbls
	if 'oth' in AllTbls:
		othTbl = AllTbls['oth']
	else:
		othTbl = loadOneTbl('oth')
		AllTbls['oth'] = othTbl
		
	c2cTbl = {}
	for oid,oth in othTbl.items():
		comm1ID = oth.comm
		comm2ID = oth.contrib
		amt = int(round(float(oth.amount)))
		negAmtFnd = False
		if amt < 0:
			amt = - amt
			negAmtFnd=True
		if not(comm1ID in keepCommTbl and comm2ID in keepCommTbl):
			# NB: can't count dropped comm2comm edges here(:
			dropComm2comm += amt
			continue
		
		otype = oth.transaction_type
				
		# NB: "against" transaction types

		if negAmtFnd or otype == '24A' or otype == '24N':
			comm1ID += '_'
			
		c2ck = (comm1ID,comm2ID)
		
		if c2ck in c2cTbl:
			c2cTbl[c2ck] += amt
		else:
			c2cTbl[c2ck] = amt
			
	for c2ck,amt in c2cTbl.items():
		(comm1ID,comm2ID) = c2ck
		anti = 0
		if comm1ID.endswith('_'):
			anti = 1
			comm1ID = comm1ID[:-1]
		nodeTbl[comm1ID][1].append( (comm2ID,amt,anti))
		
	ndTypeTbl={}
	for k,nd in nodeTbl.items():
		ndType = nd[0]
		ndTypeTbl[ndType] = ndTypeTbl.get(ndType,0)+1
		
	print 'bldGraph: NNodes=%d NDrop=%d NC2CAmtDrop=%d' % (len(nodeTbl),ndropEdge,dropComm2comm)
	for ndType in ndTypeTbl.keys():
		print '\t%s %d' % (ndType,ndTypeTbl[ndType])
						
	global AllTbls
	if 'contrib' in AllTbls:
		contribTbl = AllTbls['contrib']
	else:
		contribTbl = loadOneTbl('contrib')
		AllTbls['contrib'] = contribTbl
				
	if 'comm' in AllTbls:
		commTbl = AllTbls['comm']
	else:
		commTbl = loadOneTbl('comm')
		AllTbls['comm'] = commTbl
	
	if 'cand' in AllTbls:
		candTbl = AllTbls['cand']
	else:
		candTbl = loadOneTbl('cand')
		AllTbls['cand'] = candTbl

	candKeyTbl = {'H': [], 'P': [], 'S':[]}
	conList = []

	for nid,nd in nodeTbl.items():
		ndType,nbrList = nd
		try:
			if ndType == 'cand':
				assert len(nbrList)==0, "cand don't have out neighbors?!"
				cand = candTbl[nid]
				label = cand.candidate_name
				party = cand.party
				office = cand.candidate_office
				nodeTbl[nid].append( (0,ndType,label,party,office) )
				
				candKeyTbl[office].append(nid)
			
			elif ndType == 'comm':
				comm = commTbl[nid]
				label = comm.committee_name
				state = comm.state
				zip = comm.zip
				totOut = sum([amt for (id2,amt,anti) in nbrList ])
				nodeTbl[nid].append( (totOut,ndType,label,state,zip) )
			elif ndType == 'contrib':
				totOut = sum([amt for (id2,amt,anti) in nbrList ])
				label = nid
				nodeTbl[nid].append( (totOut,ndType,label,None,None))
				conList.append( (nid,totOut) )
		except Exception, e:
			print 'cant build node?! ',nid,ndType,len(nbrList)
			print e
			continue
			
		
	# horizontally stratify node types
	contribLong = 37. 
	candLong = 44.
	commLong = 40.5
	# use positive latitudess (:
	NMinLat = 92.
	SMaxLat = 100. 
	partyOffset = 0.25
	
	npcand = len(candKeyTbl['P'])
	nhcand = len(candKeyTbl['H'])
	nscand = len(candKeyTbl['S'])
	ncand = npcand+nhcand+nscand
	
	candLatIncr = float(SMaxLat-NMinLat) / ncand
	
	contribLatIncr = float(SMaxLat-NMinLat) / len(conList)
	
	print 'bldGraph: NPres=%d NSenate=%d NHouse=%d LatIncr=%6.3f' % (npcand,nscand,nhcand,candLatIncr)

	sortCandKeyTbl = {}
	for office in ['P','S','H']:
		nidList = candKeyTbl[office]
		nidList.sort()
		sortCandKeyTbl[office] = nidList
		
	conList.sort(key=(lambda x: x[1]),reverse=True)
	conList2 = [nid for nid,totOut in conList]
		
	allCandKeys = sortCandKeyTbl['P']+sortCandKeyTbl['S']+sortCandKeyTbl['H']
		
	nodef = DataDir + 'nodes.csv'
	print 'bldGraph: outputting nodes to',nodef
	outs = open(nodef,'w')
	outs.write('ID,type,LogOut,Label,Attr1,Attr2,Long,Lat\n')
	for nid,nd in nodeTbl.items():
		if len(nd) < 3:
			print 'empty node?!',nid,nd
			continue
		
		ndType,nbrList,ndAttrList = nd
		(totOut,ndType,label,attr1,attr2) = ndAttrList

		# log_10
		try:
			logOut = math.log(totOut,10)
		except Exception,e:
			# print 'bad totOut?!',nid,totOut,e,e.args
			logOut = 0.
			
		## longitude based on node type
		if ndType=='contrib':
			# jiggle contributors 
			long = contribLong + random.uniform(-0.5,0.5)
		elif ndType=='cand':
			# align DEM/REP
			if attr1=='DEM':
				long = candLong - partyOffset
			elif attr1=='REP':
				long = candLong + partyOffset
			else:
				long = candLong
		else:
			long = random.uniform(contribLong+2.,candLong-2.)
	
		## latidude reflects candidates' race, or contrib giving rank		
		if ndType=='cand':
			# office = attr2
			idx = allCandKeys.index(nid)
			lat =  NMinLat + idx * candLatIncr
		elif ndType=='contrib':
			if nid.startswith(SmallGivePrefix):
				strata = int(nid[-1])
				lat = NMinLat - 1. + 0.1 *strata
			else:
				idx = conList2.index(nid)
				lat = NMinLat + idx * contribLatIncr
		else:
			lat = random.uniform(NMinLat,SMaxLat)
			
		outs.write('"%s",%s,%6.3f,"%s",%s,%s,%5.2f,%5.2f\n' % (nid,ndType,logOut,label,attr1,attr2,long,lat))
	outs.close()
	
	nedge = 0
	outs = open(DataDir+'edges.csv','w')
	outs.write('Source,Target,Weight,Anti\n')
	for nid,nd in nodeTbl.items():
		if len(nd) < 3:
			print 'empty node?!',nid,nd
			continue
		ndType,nbrList,ndAttrList = nd
		for id2,amt,anti in nbrList:
			try:
				logAmt = math.log(amt,10)
			except Exception,e:
				# print 'bad totOut?!',nid,totOut,e,e.args
				logOut = 0.

			outs.write('"%s","%s",%6.3f,%d\n' % (nid,id2,logAmt,anti))
			nedge += 1
	outs.close()
	print 'bldGraph: NEdge=%d' % (nedge)

## top level routines to call

# reconn('fec')
# parseAll('fec','dormantComm.txt')
# parseAll('fec')

# loadAllTbls()
# analCand()
# bldRaceTbl()
# analComm()
# analContrib()
# analTopContrib(1000)
#comm1f = 'candComm1_uniq.csv'
#topContribf = 'comm_topContrib_1e6.csv'
#topCandf = 'comm_topCand_1e5.csv'
#bldGraph(topContribf,topCandf,comm1f)
