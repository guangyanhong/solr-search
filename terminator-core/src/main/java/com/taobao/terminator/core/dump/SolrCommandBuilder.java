package com.taobao.terminator.core.dump;

import java.io.DataOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Payload;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.schema.BinaryField;
import org.apache.solr.schema.CopyField;
import org.apache.solr.schema.DateField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;

import com.taobao.terminator.common.data.processor.BoostDataProcessor;
import com.taobao.terminator.common.data.processor.DeletionDataProcessor;
import com.taobao.terminator.core.exception.SolrXmlParseException;
import com.taobao.terminator.core.fieldx.RangeField;
import com.taobao.terminator.core.realtime.TerminatorIndexReader;
import com.taobao.terminator.core.realtime.UIDTokenStream;

public class SolrCommandBuilder {

	private static final float DEFAULT_DOCUMENT_BOOST = 1.0f;

	private static Log logger = LogFactory.getLog(SolrCommandBuilder.class);

	public static AddUpdateCommand generateAddCommand(Map<String, String> row, IndexSchema schema) {
		AddUpdateCommand addCmd = new AddUpdateCommand();
		addCmd.allowDups = false;
		addCmd.overwriteCommitted = true;
		addCmd.overwritePending = true;
		SolrInputDocument solrDoc = new SolrInputDocument();
		solrDoc.clear();
		if (row.containsKey(BoostDataProcessor.BOOST_NAME)) {
			try {
				solrDoc.setDocumentBoost(Float.parseFloat(row.get(BoostDataProcessor.BOOST_NAME)));
			} catch (NumberFormatException nfe) {
				logger.error("转化文档boost出错，将默认设为" + DEFAULT_DOCUMENT_BOOST, nfe);
				solrDoc.setDocumentBoost(DEFAULT_DOCUMENT_BOOST);
			}
			row.remove(BoostDataProcessor.BOOST_NAME);
		} else {
			solrDoc.setDocumentBoost(DEFAULT_DOCUMENT_BOOST);
		}
		for (Entry<String, String> entry : row.entrySet()) {
			if (!schema.getFields().containsKey(entry.getKey())) {
				// 忽略多余的字段，没有在schema.xml中配置的字段
				continue;
			}

			solrDoc.addField(entry.getKey(), entry.getValue());
		}

		addCmd.solrDoc = solrDoc;
		addCmd.doc = getLuceneDocument(solrDoc, schema);

		return addCmd;
	}

	public static DeleteUpdateCommand generateDeleteCommand(Map<String, String> row) {
		DeleteUpdateCommand delCmd = new DeleteUpdateCommand();
		delCmd.id = row.get(DeletionDataProcessor.DELETION_KEY);
		delCmd.fromCommitted = true;
		delCmd.fromPending = true;
		return delCmd;
	}

	public static CommitUpdateCommand generateCommitCommand(boolean optimize) {
		CommitUpdateCommand commitCmd = new CommitUpdateCommand(false);
		commitCmd.optimize = optimize;
		return commitCmd;
	}

	/**
	 * Convert a SolrInputDocument to a lucene Document.
	 * 
	 * This function should go elsewhere. This builds the Document without an
	 * extra Map<> checking for multiple values. For more discussion, see:
	 * http:/
	 * /www.nabble.com/Re%3A-svn-commit%3A-r547493---in--lucene-solr-trunk%
	 * 3A-.--
	 * src-java-org-apache-solr-common--src-java-org-apache-solr-schema--src
	 * -java
	 * -org-apache-solr-update--src-test-org-apache-solr-common--tf3931539.html
	 * 
	 * TODO: /!\ NOTE /!\ This semantics of SolrCommandBuilder function are
	 * still in flux. Something somewhere needs to be able to fill up a
	 * SolrDocument from a lucene document - SolrCommandBuilder is one place
	 * that may happen. It may also be moved to an independent function
	 * 
	 * @since solr 1.3
	 */
	public static Document getLuceneDocument(SolrInputDocument doc, IndexSchema schema) {
		Document luceneDoc = new Document();
		luceneDoc.getFields().clear();

		luceneDoc.setBoost(doc.getDocumentBoost());

		for (SolrInputField field : doc) {
			String name = field.getName();
			SchemaField sfield = schema.getFieldOrNull(name);

			if(sfield.getName().equals("_SFS_")) {
				continue;
			}
			
			boolean used = false;
			float boost = field.getBoost();

			if(sfield != null) {
				//增加Range的字段支持
				FieldType ft = sfield.getType();
				if (ft instanceof RangeField) {
					int t = ((RangeField) ft).getType();
					
					String s = field.getFirstValue().toString();
					
					try {
						if (t == RangeField.TYPE_SHORT) {
							short sh = Short.MAX_VALUE;
							try {
								sh = Short.valueOf(s);
							} catch (Exception e) {
								logger.warn("数据ValueOf异常  ==> " + s);
							}
							fillRangeField(name, luceneDoc,sh);
						} else if (t == RangeField.TYPE_INT) {
							
							int in = Integer.MAX_VALUE;
							try {
								in = Integer.valueOf(s);
							} catch (Exception e) {
								logger.warn("数据ValueOf异常  ==> " + s);
							}
							fillRangeField(name, luceneDoc,in);
						} else if (t == RangeField.TYPE_LONG) {
							long lo = Integer.MAX_VALUE;
							try {
								lo = Long.valueOf(s);
							} catch (Exception e) {
								logger.warn("数据ValueOf异常  ==> " + s);
							}
							fillRangeField(name, luceneDoc,lo);
						}
					} catch (Throwable e) {
						logger.error("填充区间字段异常",e);
					}
				}
			}

			if (sfield != null && !sfield.multiValued() && field.getValueCount() > 1) {
				String id = "";
				SchemaField sf = schema.getUniqueKeyField();
				if (sf != null) {
					id = "[" + doc.getFieldValue(sf.getName()) + "] ";
				}
				SolrXmlParseException e = new SolrXmlParseException(id + "multiple values encountered for non multiValued field " + sfield.getName()
						+ ": " + field.getValue());
				logger.error("Bad Solr xml format.", e);
				throw e;
			}

			boolean hasField = false;
			for (Object v : field) {
				if (v == null) {
					continue;
				}

				String val = null;
				hasField = true;
				boolean isBinaryField = false;
				if (sfield != null && sfield.getType() instanceof BinaryField) {
					isBinaryField = true;
					BinaryField binaryField = (BinaryField) sfield.getType();
					Field f = binaryField.createField(sfield, v, boost);
					if (f != null)
						luceneDoc.add(f);
					used = true;
				} else {
					if (sfield != null && v instanceof Date && sfield.getType() instanceof DateField) {
						DateField df = (DateField) sfield.getType();
						val = df.toInternal((Date) v) + 'Z';
					} else if (v != null) {
						val = v.toString();
					}

					if (sfield != null) {
						used = true;
						Field f = sfield.createField(val, boost);
						if (f != null) {
							luceneDoc.add(f);
						}
					}
				}

				List<CopyField> copyFields = schema.getCopyFieldsList(name);
				for (CopyField cf : copyFields) {
					SchemaField destinationField = cf.getDestination();
					if (!destinationField.multiValued() && luceneDoc.get(destinationField.getName()) != null) {
						SolrXmlParseException e = new SolrXmlParseException("multiple values encountered for non multiValued copy field "
								+ destinationField.getName() + ": " + val);
						logger.error("Bad xml format.", e);
						throw e;
					}

					used = true;
					Field f = null;
					if (isBinaryField) {
						if (destinationField.getType() instanceof BinaryField) {
							BinaryField binaryField = (BinaryField) destinationField.getType();
							binaryField.createField(destinationField, v, boost);
						}
					} else {
						f = destinationField.createField(cf.getLimitedValue(val), boost);
					}
					if (f != null) {
						luceneDoc.add(f);
					}
				}

				boost = 1.0f;
			}

			if (!used && hasField) {
				SolrXmlParseException e = new SolrXmlParseException("ERROR:unknown field '" + name + "'");
				logger.error("bad xml format.", e);
				throw e;
			}
		}

		for (SchemaField field : schema.getRequiredFields()) {
			if(field.getName().equals("_SFS_")) {
				continue;
			}
			
			if (luceneDoc.getField(field.getName()) == null) {
				if (field.getDefaultValue() != null) {
					luceneDoc.add(field.createField(field.getDefaultValue(), 1.0f));
				} else {
					String id = schema.printableUniqueKey(luceneDoc);
					String msg = "Document [" + id + "] missing required field: " + field.getName();
					SolrXmlParseException e = new SolrXmlParseException(msg);
					logger.error("bad xml format.", e);
					throw e;
				}
			}
		}

		SchemaField uniqueKeyField = schema.getUniqueKeyField();
		if (uniqueKeyField != null) {
			String keyName = uniqueKeyField.getName();
			String uidStr = luceneDoc.get(keyName);

			try {
				long uid = Long.valueOf(uidStr);
				fillDocumentID(luceneDoc, uid);
			} catch (Exception e) {
				// 忽略掉异常，大不了就不做
			}
		}
		SchemaField sf = null;
		try {
			sf = schema.getField("_SFS_");
		} catch (SolrException e) {
			
		}
		
		// 增加排序字段的支持
		if (sf != null) {
			String sfs = sf.getDefaultValue();
			
			if(sfs == null || sfs.trim().equals("")) {
				throw new RuntimeException("Field [_SFS_]'s default value is empty!");
			}
			
			// i_c:I,a_p:S,c_n:L
			String[] ps = sfs.split(",");

			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			DataOutputStream dout = new DataOutputStream(bout);

			for (String p : ps) {
				String[] items = p.split(":");
				String fName = items[0].trim();
				String fType = items[1].trim();
				
				String svalue = doc.getFieldValue(fName).toString();
				if(svalue == null || svalue.trim().equals("")) {
					svalue = "1";
				}
				
				try { 
					if (fType.equals("S")) {
						short sh = Short.MAX_VALUE;
						try {
							sh = Short.valueOf(svalue);
						} catch (Exception e) {
							logger.warn("数据ValueOf异常  ==> " + svalue);
						}
						dout.writeShort(sh);
					} else if (fType.equals("I")) {
						int in = Integer.MAX_VALUE;
						try {
							in = Integer.valueOf(svalue);
						} catch (Exception e) {
							logger.warn("数据ValueOf异常  ==> " + svalue);
						}
						dout.writeInt(in);
					} else if (fType.equals("L")) {
						long lo = Integer.MAX_VALUE;
						try {
							lo = Long.valueOf(svalue);
						} catch (Exception e) {
							logger.warn("数据ValueOf异常  ==> " + svalue);
						}
						dout.writeLong(lo);
					} else if(fType.equals("D")) { //日期类型，只是如果保存形如20121220这样的8位数的话，在Payload计算的时候会出现精度的问题，所以去除前缀
						Date date = new SimpleDateFormat("yyyyMMdd").parse(svalue);
						Calendar calendar = Calendar.getInstance();
						calendar.setTime(date);
						calendar.set(Calendar.HOUR_OF_DAY, 0);
						calendar.set(Calendar.MINUTE, 0);
						calendar.set(Calendar.SECOND, 0);
						calendar.set(Calendar.MILLISECOND, 0);
						
						long days = (calendar.getTime().getTime() - startTime) / (1000 * 60 * 60 * 24);
						dout.writeInt((int)days);
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
            //TODO
			byte[] b = bout.toByteArray();

			// 填充存储排序字段的值的payload字段
			fillSortFields(luceneDoc, b);
		}
		
		return luceneDoc;
	}
	
	protected static Calendar startCalendar = Calendar.getInstance();
	protected static long startTime = 0L;
	
	static {
		try {
			Date startDate = new SimpleDateFormat("yyyyMMdd").parse("20030101");
			startCalendar = Calendar.getInstance();
			startCalendar.setTime(startDate);
			startCalendar.set(Calendar.HOUR_OF_DAY, 0);
			startCalendar.set(Calendar.MINUTE, 0);
			startCalendar.set(Calendar.SECOND, 0);
			startCalendar.set(Calendar.MILLISECOND, 0);
			
			startTime = startCalendar.getTime().getTime();
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Date endDate = new SimpleDateFormat("yyyyMMdd").parse("20000101");

		Calendar endCalendar = Calendar.getInstance();
		endCalendar.setTime(endDate);
		endCalendar.set(Calendar.HOUR_OF_DAY, 0);
		endCalendar.set(Calendar.MINUTE, 0);
		endCalendar.set(Calendar.SECOND, 0);
		endCalendar.set(Calendar.MILLISECOND, 0);

		System.out.println((endCalendar.getTime().getTime() - startCalendar.getTime().getTime()) / (1000 * 60 * 60 * 24));
		
		
		System.out.println(1.0/36524);
	}

	public static void fillDocumentID(Document doc, long id) {
		Field uidField = new Field(TerminatorIndexReader.UID_TERM.field(), new UIDTokenStream(id));
		uidField.setOmitNorms(true);
		doc.add(uidField);
	}

	public static void fillSortFields(Document doc,byte[] b) {
		Field sf = new Field("_SFS_",new SFTokenStream(b));
		sf.setOmitNorms(true);
		doc.add(sf);
	}
	
	public static class SFTokenStream extends TokenStream {
		private boolean returnToken = false;

		private PayloadAttribute payloadAttr;
		private TermAttribute termAttr;

		public SFTokenStream(byte[] b) {
			payloadAttr = (PayloadAttribute) addAttribute(PayloadAttribute.class);
			payloadAttr.setPayload(new Payload(b));
			
			termAttr = (TermAttribute) addAttribute(TermAttribute.class);
			termAttr.setTermBuffer("_SFS_VALUE_");
			
			returnToken = true;
		}

		@Override
		public boolean incrementToken() throws IOException {
			if (returnToken) {
				returnToken = false;
				return true;
			} else {
				return false;
			}
		}
	}
	
	public static void fillRangeField(String fieldName, Document doc, int num) {
		Field f = new Field("RF_I_" + fieldName, new IntRangeTokenStream(num, "RF_I_" + fieldName));
		f.setOmitNorms(true);
		doc.add(f);
	}

	public static void fillRangeField(String fieldName, Document doc, short num) {
		Field f = new Field("RF_S_" + fieldName, new ShortRangeTokenStream(num, "RF_S_" + fieldName));
		f.setOmitNorms(true);
		doc.add(f);
	}

	public static void fillRangeField(String fieldName, Document doc, long num) {
		Field f = new Field("RF_L_" + fieldName, new LongRangeTokenStream(num, "RF__L_" + fieldName));
		f.setOmitNorms(true);
		doc.add(f);
	}

	public static class IntRangeTokenStream extends TokenStream {

		private boolean returnToken = false;

		private PayloadAttribute payloadAttr;
		private TermAttribute termAttr;

		public IntRangeTokenStream(int num, String value) {
			byte[] buffer = new byte[4];

			buffer[0] = (byte) (num);
			buffer[1] = (byte) (num >> 8);
			buffer[2] = (byte) (num >> 16);
			buffer[3] = (byte) (num >> 24);

			payloadAttr = (PayloadAttribute) addAttribute(PayloadAttribute.class);
			payloadAttr.setPayload(new Payload(buffer));

			termAttr = (TermAttribute) addAttribute(TermAttribute.class);
			termAttr.setTermBuffer(value);

			returnToken = true;
		}

		@Override
		public boolean incrementToken() throws IOException {
			if (returnToken) {
				returnToken = false;
				return true;
			} else {
				return false;
			}
		}
	}

	public static class ShortRangeTokenStream extends TokenStream {

		private boolean returnToken = false;

		private PayloadAttribute payloadAttr;
		private TermAttribute termAttr;

		public ShortRangeTokenStream(short num, String value) {
			byte[] buffer = new byte[2];

			buffer[0] = (byte) (num);
			buffer[1] = (byte) (num >> 8);

			payloadAttr = (PayloadAttribute) addAttribute(PayloadAttribute.class);
			payloadAttr.setPayload(new Payload(buffer));

			termAttr = (TermAttribute) addAttribute(TermAttribute.class);
			termAttr.setTermBuffer(value);

			returnToken = true;
		}

		@Override
		public boolean incrementToken() throws IOException {
			if (returnToken) {
				returnToken = false;
				return true;
			} else {
				return false;
			}
		}
	}
	
	public static class LongRangeTokenStream extends TokenStream {

		private boolean returnToken = false;

		private PayloadAttribute payloadAttr;
		private TermAttribute termAttr;

		public LongRangeTokenStream(long num, String value) {
			byte[] buffer = new byte[8];

			buffer[0] = (byte) (num);
			buffer[1] = (byte) (num >> 8);
			buffer[2] = (byte) (num >> 16);
			buffer[3] = (byte) (num >> 24);
			buffer[4] = (byte) (num >> 32);
			buffer[5] = (byte) (num >> 40);
			buffer[6] = (byte) (num >> 48);
			buffer[7] = (byte) (num >> 56);

			payloadAttr = (PayloadAttribute) addAttribute(PayloadAttribute.class);
			payloadAttr.setPayload(new Payload(buffer));

			termAttr = (TermAttribute) addAttribute(TermAttribute.class);
			termAttr.setTermBuffer(value);

			returnToken = true;
		}

		@Override
		public boolean incrementToken() throws IOException {
			if (returnToken) {
				returnToken = false;
				return true;
			} else {
				return false;
			}
		}
	}
}
