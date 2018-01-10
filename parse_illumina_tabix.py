import os
import sys
import sqlite3 as lite
import tabix


os.system('clear')

def quote_identifier(identifier):
	return b"\"" + identifier.replace(b"\"", b"\"\"") + b"\""

def character_replace(line,sep = '\t'):
	character_list = ['.',':','(',')',' ',',','#','-']
	if sep in character_list:
		character_list.remove(sep)
	for entry in character_list:
		line = line.replace(entry,'_')

	return(line)


def start_with_digit(line_list):
	new_list = []
	for line in line_list:
		if(len(line) > 0):
			if line[0].isdigit():
				line = '_'+line
		new_list.append(line)
	return(new_list)


wd = os.getcwd()
print(wd)

file_list = os.listdir(wd)
print 'file_list'
print(file_list)
full_file_parse_list = []
for file_name in file_list:
	if '.txt' in file_name or '.vcf' in file_name or '.csv' in file_name:
		if '.vcf.gz' not in file_name and 'id_hit' not in file_name and 'slite' not in file_name and 'result' not in file_name:
			full_file_parse_list.append(file_name)

print '\nfull file_list'
print full_file_parse_list
#raw_input()

file_list = os.listdir(wd)
#print file_list
gz_file_list = []
for file_name in file_list:
	if('genome.vcf.gz' in file_name and 'tbi' not in file_name):
		gz_file_list.append(file_name)
print 'gz file list'
print gz_file_list
#print(file_parse_list)
#raw_input()

subset = 'hg19'
#subset = 'CNV'
subset = 'hg38'
subset = 'SV'

try:
	subset = sys.argv[1]
except:
	subset = 'tabix'

first = True
first = False


#insert_data = False

db_path = 'db_%s.slite' %(subset)
subset = 'HumanOmni'

reduced_file_list = ['0.iDDNA_Parser.OF01052018.txt','HumanOmni5-4-v1-0-D.csv']
if subset != "tabix":
	for entry in full_file_parse_list:
		if subset in entry:
			reduced_file_list.append(entry)

file_parse_list = reduced_file_list
print '\nfile_parse_list'
print subset
print(file_parse_list)
#raw_input()
#db_path = 'db.slite'


print '\n'
print(db_path)

#raw_input()

#db_path = 'db_2.slite'

con = lite.connect(db_path)
#con.text_factory = bytes
cur = con.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")


#print(file_parse_list)

#raw_input()
illumina_file_list = ['hg19.hybrid.vcf', 'hg19_NA12877.vcf', 'hg19_NA12878.vcf', 'hg38.hybrid.vcf', 'hg38_NA12877.vcf', 'hg38_NA12878.vcf']

illumina_file_list_inserted = []
#illumina_file_list_inserted = ["hg19.hybrid.vcf","hg19_NA12877.vcf","hg19_NA12878.vcf","hg38.hybrid.vcf","hg38_NA12877.vcf",'hg38_NA12878.vcf']
#illumina_file_list_inserted = illumina_file_list

illumina_insert = False
#illumina_file_list_inserted = ['hg19.hybrid.vcf', 'hg19_NA12877.vcf', 'hg19_NA12878.vcf', 'hg38.hybrid.vcf', 'hg38_NA12877.vcf', 'hg38_NA12878.vcf']

genome_file_list = ["NA12889_S1.genome.vcf","NA12892_S1.genome.vcf"]
S1_file_list = ["NA12889_S1.vcf","NA12892_S1.vcf"]
CNV_file_list = ['NA12889_S1.CNV.VFResults.txt']
inserted_file = []
insert_file = open('inserted_file','a')


cmd = "select name from sqlite_master where type = 'table';"
cur.execute(cmd)
table_query = cur.fetchall()
print(table_query)
table_list = []
for entry in table_query:
	table_list.append(str(entry[0]))

print '\ntable_list'
print table_list
raw_input()

forced_list = ['HumanOmni5Exome_4_v1_1_B_auxilliary_file_txt']
#forced_list = []
####################################### 
insert_data = False
#insert_data = False
force_insert = False
run_index = True
if insert_data == True:
	for file_name in file_parse_list:
		table_name = character_replace(file_name)
		print table_name
		
		if table_name not in table_list or table_name in forced_list:
			parse_db = True
			new = True
			heading_index = 100
			table_name = 'new'
			cmd = 'done'
			add = ''
			print(file_name)
			insert = True
			create = True
			indexed_columns = []
			if file_name == '0.iDDNA_Parser.OF01052018.txt':
				heading_index = 2
				start_index = 3
				sep = '\t'
				table_name = 'Parser'
				insert = False
				create = False
				sql_header_line = """Create Table if not exists Parser 
				(Prog__N_ INT PRIMARY KEY,
				 rs_SNPIP_ VARCHAR(20),
				 Genotype CHARACTER(5),
				 REF VARCHAR(20), ALT VARCHAR(20), 
				 Gene_name__Label_ VARCHAR(255), 
				 Chromosome VARCHAR(20), 
				 Chromos__Location VARCHAR(20), 
				 Position BIGINT, 
				 Telomere VARCHAR(20), 
				 ILLUMINA_IDENTIFIER VARCHAR(20), 
				 _23andme_IDENTIFIER VARCHAR(20), 
				 AFFY_IDENTIFIER VARCHAR(20), 
				 African__AFR_ VARCHAR(5), 
				 South_Asian__SAS_ VARCHAR(5), 
				 Chinese__CHB_ VARCHAR(5), 
				 East_Asian__EAS_ VARCHAR(5), 
				 American__CEU_ VARCHAR(5), 
				 South_American__AMR_ VARCHAR(5), 
				 European__EUR_ VARCHAR(5), 
				 Scientific_ref__Population VARCHAR(255), 
				 Scientific_ref__Population_2 VARCHAR(255), 
				 API VARCHAR(4))"""
				#parse_db = False
				new = False

			if file_name in illumina_file_list:
				heading_index = 43
				if 'hybrid' not in file_name:
					heading_index = 48
				sep = '\t'
				table_name = character_replace(file_name.replace('.vcf',''))
	
				if file_name in illumina_file_list_inserted:
					insert = False
				sql_header_line = """Create Table if not exists %s 
				(_CHROM VARCHAR(6), 
				POS BIGINT, 
				ID VARCHAR(20), 
				REF VARCHAR(20), 
				ALT VARCHAR(20), 
				QUAL VARCHAR(20), 
				FILTER VARCHAR(20), 
				INFO VARCHAR(255), 
				FORMAT VARCHAR(20), 
				FILE_INFO VARCHAR(20),
				FILE VARCHAR(20))""" %(table_name)
				add = 'file'
				new = False

				#parse_db = False
			#if "SV" in file_name or 'genome' in file_name or 'S1.vcf' in file_name:
			if subset in file_name:

				heading_index = 10000
				sep = '\t'
				table_name = character_replace(file_name)
			
				sql_header_line = """Create Table if not exists %s 
				(_CHROM VARCHAR(6), 
				POS BIGINT, 
				ID VARCHAR(20), 
				REF VARCHAR(20), 
				ALT VARCHAR(20), 
				QUAL VARCHAR(20), 
				FILTER VARCHAR(20),
				INFO VARCHAR(255), 
				FORMAT VARCHAR(20), 
				FILE_INFO VARCHAR(20),
				FILE VARCHAR(20))""" %(table_name)
				add = 'file'
				new = False
			if 'CNV' in file_name:

				heading_index = 0
				sep = '\t'
				table_name = character_replace(file_name)
			
				sql_header_line = """Create Table if not exists %s 
				(_CHROM VARCHAR(6), 
				POS BIGINT, 
				REF VARCHAR(20), 
				ALT VARCHAR(20), 
				CountRef INT, 
				CountAlt INT,
				file VARCHAR(50))""" %(table_name)
				add = 'file'
				new = False
			if 'HumanOmni' in file_name:

				insert = False
				create = False
				heading_index = 7

				sql_header_line = """Create Table if not exists HumanOmni5_4_v1_0_D_csv 
				(
					IlmnID VARHCAR(50) PRIMARY KEY, 
					Name INT, 
					IlmnStrand VARHCAR(5),
					SNP VARHCAR(5),
					AddressA_ID BIGINT,
					AlleleA_ProbeSeq VARHCAR(500),
					AddressB_ID BIGINT,
					AlleleB_ProbeSeq VARCHAR(500),
					GenomeBuild INT,
					Chr VARCHAR(5),
					MapInfo BIGINT,
					Ploidy VARCHAR(20),
					Species VARCHAR(50),
					Source VARCHAR(50),
					SourceVersion VARCHAR(50),
					SourceStrand VARCHAR(5),
					SourceSeq VARCHAR(5000),
					TopGenomicSeq VARCHAR(5000),
					BeadSetID INT,
					RefStrand VARCHAR(20),
	                Exp_Clusters INT
				)""" 

				
				table_name = character_replace(file_name)
				#sql_header_line = False	
				#add = 'file'
				new = False
				if 'auxilliary-file' in file_name:
					table_name = character_replace(file_name)

					heading_index = 0
					sep = '\t'
					sql_header_line = """Create Table if not exists %s 
				(
					Name VARHCAR(50) PRIMARY KEY, 
					RsID VARCHAR(50),
					file VARCHAR(255)
				)""" %(table_name)
					insert = True
					create = True
					new = False
					add = 'file'
					indexed_columns = ['Name','RsID']
			

			index_cmd_list = []
			for index_name in indexed_columns:
				cmd =  'DROP INDEX IF EXISTS %s_%s_index' %(table_name,index_name)
				index_cmd_list.append(cmd)
				cmd = 'CREATE INDEX %s_%s_index ON %s (%s);' %(table_name,index_name,table_name,index_name)
				index_cmd_list.append(cmd)



				#parse_db = False
			#create = True
			
			if parse_db == True:

				read_file = open(file_name,'r')
				cnt = 0
				line = read_file.readline()
				line = line.replace('\r\n', '')
				line = line.replace('\n','')
				#raw_input(line)
				while line:
					#print(cnt)
					if new == True:
						print file_name
						print cnt
						#raw_input(line)
					if(line[0:2] == '#C'):
						heading_index = cnt
					if cnt == heading_index:
						print '\n\nHEADING\n'
						header_line = character_replace(line,sep)
						header_list = header_line.split(sep)
						print(header_list)
						header_list = start_with_digit(header_list)
						print(header_list)
						try:
							last_index = header_list.index('')
						except:
							last_index = len(header_list)
						header_list = header_list[:last_index]
						print '\n'

						if create == True:
							drop_cmd = 'DROP TABLE IF EXISTS %s' %(table_name)
							print drop_cmd
							if sql_header_line == False :
							
								sql_header_line = 'Create Table if not exists %s (%s)' %(table_name,', '.join(header_list))
							
							print '\n'
							print sql_header_line
							#raw_input()
							raw_input('\n\nabout to create new table - enter to continue\n\n')
							cur.execute(drop_cmd)
							cur.execute(sql_header_line)
							inserted_file.append(file_name)
							insert_file.write(file_name)
					
					if cnt > heading_index:
						if insert == True:
							if new == True:

								print(line)
							line_list = line.replace('\r\n','').replace('"','').replace('\n','').split(sep)
							line_list = line_list[:last_index]
							if(len(line_list) != len(header_list)):
								print line
							if(add == 'file'):
								line_list.append(file_name)
							if(line_list[0] != ''):

								cmd = 'INSERT INTO %s VALUES("%s")' %(table_name,'","'.join(line_list))
								if cnt % 1000000 == 0 or cnt == heading_index + 1:
									print cnt
									print cmd
								try:
									cur.execute(cmd)
					
								except:
									print cnt
									print(line)
									print(cmd)
									#raw_input()
									raw_input('Insert ERROR')
									cur.execute(cmd)
								#	insert = False
								#	break
						else:
							break
					

					line = read_file.readline()
					cnt += 1
					
					
					#if cnt > 5:
					#	break
			print cnt
			print cmd

			con.commit()
			if run_index == True:
				for cmd in index_cmd_list:
					print cmd
					cur.execute(cmd)
				con.commit()
			#raw_input(file_name)

cmd = "select name from sqlite_master where type = 'table';"
cur.execute(cmd)
table_query = cur.fetchall()
print(table_query)
table_list = []
for entry in table_query:
	table_list.append(str(entry[0]))
#raw_input(table_list)

remove_list = ['Parser','hit']
search_table_list = []
for entry in table_list:
	if entry not in remove_list:
		search_table_list.append(entry)
#table_list = ['Parser','illumina']

run_count = False
run_test = True
for table_name in table_list:
	print table_name
	cmd = 'PRAGMA table_info(%s);' %(table_name)
	#print(cmd)
	cur.execute(cmd)
	heading = cur.fetchall()
	##print heading

	heading_list = []
	for entry in heading:
		heading_list.append(entry[1])
	print heading_list
	if run_count == True:
		cmd = 'SELECT count(*) from %s;' %(table_name)
		cur.execute(cmd)
		count_db = cur.fetchall()
		print count_db 
	if run_test == True:
		cmd = 'SELECT * from %s;' %(table_name)
		cur.execute(cmd)
		count_db = cur.fetchone()
		print count_db 
raw_input()

cmd = 'SELECT Chromosome,Position,Genotype,Prog__N_,rs_SNPIP_ FROM Parser'
#print(cmd)
#raw_input(cmd)
cur.execute(cmd)
main_query_list = cur.fetchall()
print(main_query_list[1])
cnt = 0
cmd = 'SELECT * FROM hg38_hybrid WHERE _CHROM == "chr2"'

search_list = []
duplicates = 0
hits = 0
uniques = 0
hit_list = []



id_file = open('id_hit_%s.txt' %(subset),'w')


run_index = False
if run_index == True:
	cmd =  'DROP INDEX IF EXISTS Chr_index'
	#print(cmd)
	#cur.execute(cmd)

	cmd = 'CREATE INDEX Chr_index ON HumanOmni5_4_v1_0_D_csv (Chr);'
	#print cmd
	#cur.execute(cmd)

	cmd = 'CREATE INDEX MapInfo_index ON HumanOmni5_4_v1_0_D_csv (MapInfo);'
	#print cmd
	#cur.execute(cmd)


	cmd = 'CREATE INDEX Name_index ON HumanOmni5_4_v1_0_D_csv (Name);'
	print cmd
	cur.execute(cmd)

	cmd = 'CREATE INDEX Chr_MapInfo_index ON HumanOmni5_4_v1_0_D_csv (Chr, MapInfo);'
	#print cmd
	#cur.execute(cmd)

	raw_input('enter to commit')
	con.commit()


########################################
run_query = False
if run_query == True:
	create_hit_table = False
	if create_hit_table == True:
		#if 'hit' not in table_list:
		table_name = 'hit'
		sql_header_line = """Create Table if not exists %s
					(_CHROM VARCHAR(6), 
					POS BIGINT, 
					ID VARCHAR(20), 
					REF VARCHAR(20), 
					ALT VARCHAR(20), 
					QUAL VARCHAR(20), 
					FILTER VARCHAR(20), 
					INFO VARCHAR(255), 
					FORMAT VARCHAR(20), 
					FILE_INFO VARCHAR(20),
					FILE VARCHAR(20))""" %(table_name)

		if subset == 'CNV':
			sql_header_line = """Create Table if not exists %s 
				(_CHROM VARCHAR(6), 
				POS BIGINT, 
				REF VARCHAR(20), 
				ALT VARCHAR(20), 
				CountRef INT, 
				CountAlt INT,
				file VARCHAR(50))""" %(table_name)

		drop_cmd = 'DROP TABLE IF EXISTS %s' %(table_name)
		print drop_cmd
		cur.execute(drop_cmd)
		print(sql_header_line)
		cur.execute(sql_header_line)


	tabix_query = False
	db_query = False
	illumina_query = True
	query_hit = 0
	rs_hits = 0
	full_hits = 0


	for query in main_query_list:

	
		print '\n\n'
		print cnt
		#print duplicates
		print uniques
		print hits
		print full_hits
		print rs_hits
		print '\n'
		cnt += 1 
		chr = 'chr%s' %(query[0])
		pos_q = str(query[1])
		rs_id = query[4]
		search = '%s_%s' %(chr,pos_q)
		print search
		if search not in search_list and pos_q.isdigit():
			query_hit = 0
			uniques += 1
			search_list.append(search)
			pos_list = pos_q.split('-')
			print pos_list
			if len(pos_list) > 1:
				pos_range = range(int(pos_list[0]),int(pos_list[len(pos_list)-1])+1)
			else:
				#pos_range = range(int(pos_q)-1,int(pos_q)+2)
				pos_range = [int(pos_q)]
			print(pos_range)
			#raw_input(pos_range)
			ilID_list = []
			for pos in pos_range:
				#pos = int(query[1])
				#chr = 'chr1'

				#pos = 727477
				#chr1	727477
				#print chr
				#print pos
				#print search
				




				if tabix_query == True:
					#cmd = 'tabix'

					#print gz_file_list
					for file_name in gz_file_list:
						print file_name
						tb = tabix.open(file_name)
						#print tb
						chr = chr.replace(' ','')
						print chr,pos-1,pos
				

						#try:
						records = tb.query(chr,pos-1,pos)
						#print records
						
						record_list = []
						for record in records:

							entry_list = []
							for e in record:
								entry_list.append(e)

							entry_list.append(file_name)
							record_list.append(entry_list)
						print len(record_list)
						if len(record_list) > 0:
							for entry_list in record_list:
								print entry_list[1]

								cmd = 'INSERT INTO %s VALUES("%s")' %('hit','","'.join(entry_list))
								#print(cmd)
								cur.execute(cmd)
								full_hits += 1
							query_hit = 1
						#except:
					#		print 'query failed'
					#		raw_input()
						#raw_input()

					#entry = os.system(cmd)
					#print entry
					#raw_input('tabix')

				if db_query == True:

					for table_name in search_table_list:
						#print table_name
						cmd = 'SELECT * FROM %s WHERE _CHROM = "%s" AND POS = %i' %(table_name,chr,pos)
						#cmd = 'SELECT * FROM %s WHERE POS = %s' %(table_name,pos)

						#cmd = 'SELECT * FROM hg38_hybrid WHERE POS == "188999354"'

						print(cmd)
						#raw_input(cmd)
						cur.execute(cmd)
						query_list = cur.fetchall()
						print(query_list)
						if(len(query_list) > 0) :
							for entry in query_list:
								entry_list = []
								for e in entry:
									entry_list.append(str(e))
								print(entry_list)
								cmd = 'INSERT INTO %s VALUES("%s")' %('hit','","'.join(entry_list))
								print(cmd)
								cur.execute(cmd)
							#id_file.write(str(query[3]))


						try:
							print(query_list[0])
							query_hit = 1
							#hits += 1
							#hit_list.append(query_list)

						#raw_input()
						except:
							print 'no hits'
						#raw_input()
			if(query_hit == 1):
				hits += 1
				id_file.write(str(query[3])+'\n')

		else:
			duplicates += 1
			if query_hit == 1:
				id_file.write(str(query[3])+'\n')
con.commit()






#########################################
run_result = True
illumina_query = True
h = 0
g_hit = 0
g_0 = 0
rs_hit = 0
rs_0 = 0
rs_g = 0
q = 0
e = 0
r = 0
rs_hits = 0
if run_result == True:
	print 'result'

	cmd = 'ALTER TABLE Parser ADD genome VARCHAR(255);'
	#print cmd
	#cur.execute(cmd)

	cmd = 'ALTER TABLE Parser ADD Chr_Pos_2_IlmnID VARCHAR(255);'
	#print cmd
	#cur.execute(cmd)


	cmd = 'ALTER TABLE Parser ADD RsID_2_IlmnID VARCHAR(255);'
	print cmd
	#cur.execute(cmd)


	#raw_input()
	for query in main_query_list:


		print q,h,rs_hit,rs_0,g_hit,g_0,rs_g,e,r,rs_hits

		q += 1 
		print '\n\n'
		print query
		chr = 'chr%s' %(str(query[0]))
		pos_q = str(query[1])
		
		genotype = query[2]
		rs = query[4]
		rs_id = query[4]
		pos_list = pos_q.split('-')
		P_id = query[3]
		if(pos_list[0].isdigit()):
			#print pos_list
			if len(pos_list) > 1:
				pos_range = range(int(pos_list[0]),int(pos_list[len(pos_list)-1])+1)
			else:
				#pos_range = range(int(pos_q)-1,int(pos_q)+2)
				pos_range = [int(pos_q)]
			#print(pos_range)
			final_list = []
			source_list = []
			update_list = [[],[],[],[]]
			cp_ilID_list = []
			rs_ilID_list = []
			i_q = 0
			for pos in pos_range:
				print pos
			#raw_input(pos_range)



				

				if illumina_query == True:
					#print '\n\n\n'
					#print [query[0]]
					#if "X" not in query[0]:
					#print '\n'
					cmd = 'SELECT * FROM HumanOmni5_4_v1_0_D_csv WHERE Chr = "%s" AND MapInfo = %i' %(query[0],pos)
					#print cmd
					cur.execute(cmd)
					result = cur.fetchall()
					#print result
					for entry in result:
						#print entry
						cp_ilID_list.append(entry[0])
					if len(result) > 0:
						#raw_input('hit - enter to continue')
						#query_hit += 1
						i_q = 1
					#print '\n'
					cmd = 'SELECT * FROM HumanOmni5Exome_4_v1_1_B_auxilliary_file_txt WHERE RsID = "%s"' %(rs_id)
					#print cmd
					cur.execute(cmd)
					result = cur.fetchall()
					#print result
					#for entry in result:
						#print entry
						#ilID_list.append(entry[0])
					if len(result) > 0:
						for result_entry in result:
							result_entry[0]
							#print '\n'
							cmd = 'SELECT * FROM HumanOmni5_4_v1_0_D_csv WHERE Name = "%s"' %(result_entry[0])
							#print cmd
							cur.execute(cmd)
							sub_result = cur.fetchall()
							#print sub_result
							for sub_entry in sub_result:
								#print sub_entry
								rs_ilID_list.append(sub_entry[0])
							if len(sub_result) > 0:


								#raw_input('rs_hit - enter to continue')
								rs_hits += 1
								i_q = 1
			#if i_q == 1:
				#print '\n'
				#print cp_ilID_list

				#print '\n'
				#if len(f_rs_ilID_list) > 1 or len(f_cp_ilID_list) > 1:
				#	print f_cp_ilID_list
				#	print cp_ilID_line
			#		print f_rs_ilID_list
				#	print rs_ilID_line
					#raw_input()
					#raw_input()




				cmd = 'SELECT * FROM hit WHERE _CHROM = "%s" AND POS = %i' %(chr,pos)
				cur.execute(cmd)
				result = cur.fetchall()
				#print result

				for entry in result:
					
					final_list.append(entry[2:5])
					source_list.append(entry[10])
					print entry
					print '\n'
					#print entry[8]
					if rs == entry[2]:
						#print 'rs_hit'
						rs_hit += 1
					if genotype in entry[8]:
						print 'genotype hit'
						g_hit +=1
					if rs == entry[2] and genotype in entry[8]:
						rs_g += 1
					if rs == entry[2] and genotype not in entry[8]:
						rs_0 += 1
					if rs != entry[2] and genotype in entry[8]:
						g_0 += 1
			if len(result) > 0:
				h += 1
				print final_list
				print set(final_list)

					#else :
					
					#entry_list = []
					
			for g in set(final_list):
				print g
				i = 0
				for f in g:
					print f
					update_list[i].append(f)
					i += 1
					#update_list = list(set(final_list))[1]
			print update_list
			ref = ', '.join(update_list[1])
			alt = ', '.join(update_list[2])
			ill_id = ', '.join(update_list[0])
			genome = ', '.join(source_list)
			#raw_input(genome)

			f_cp_ilID_list = []
			for entry in set(cp_ilID_list):
				f_cp_ilID_list.append(entry)
			cp_ilID_line = ', '.join(f_cp_ilID_list)

			#print rs_ilID_list
			f_rs_ilID_list = []
			for entry in set(rs_ilID_list):
				f_rs_ilID_list.append(entry)
			rs_ilID_line = ', '.join(f_rs_ilID_list)

			cmd = 'UPDATE Parser SET REF = "%s", ALT = "%s", ILLUMINA_IDENTIFIER = "%s", genome = "%s", Chr_Pos_2_IlmnID = "%s", RsID_2_IlmnID = "%s" WHERE Prog__N_ = %s' %(ref,alt,ill_id,genome,cp_ilID_line,rs_ilID_line,P_id)
			print(cmd)
			cur.execute(cmd)
			if len(set(final_list)) >1:
				r += 1
				print 'redundancy'
				#raw_input()
			#if len(pos_range) > 1:
			#	raw_input('pos')

						#raw_input()
		else:
			print query
			e += 1
			#raw_input('no position')

	con.commit()
run_write = True
if run_write == True:
	cmd = 'SELECT ALT,REF,ILLUMINA_IDENTIFIER,genome,Chr_Pos_2_IlmnID,RsID_2_IlmnID FROM Parser'
	cur.execute(cmd)
	write_list = cur.fetchall()	
	new_write_list = []
	for entry in write_list:
		#print entry
		entry_list = []
		for e in entry:
			#print e
			entry_list.append(str(e))
		new_write_list.append('\t'.join(entry_list)+'\n')
	for entry in new_write_list:
		print entry
		#raw_input()
	write_file = open('result.txt','w')
	write_file.writelines(new_write_list)
	write_file.close()


con.commit()
