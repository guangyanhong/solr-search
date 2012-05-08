package com.taobao.terminator.core.index.importer;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.BinaryField;
import org.apache.solr.schema.CopyField;
import org.apache.solr.schema.DateField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;

import com.taobao.terminator.core.exception.SolrXmlParseException;

public class SolrCommandProcessor {
	private static Log logger = LogFactory.getLog(SolrCommandProcessor.class);
	private XMLStreamReader reader;
	private SolrInputDocument solrDoc;
	private AddUpdateCommand addCmd;
	private DeleteUpdateCommand delCmd;
	private IndexSchema schema;
	private Document luceneDoc;
	private SolrCore solrCore;

	public SolrCommandProcessor() {
		this.solrDoc = new SolrInputDocument();
		this.addCmd = new AddUpdateCommand();
		this.delCmd = new DeleteUpdateCommand();
		this.luceneDoc = new Document();
	}

	public SolrCommandProcessor(XMLStreamReader reader, IndexSchema schema,SolrCore solrCore) {
		this();
		this.reader = reader;
		this.schema = schema;
		this.solrCore = solrCore;
	}

	public void process() throws XMLStreamException, IOException {
		this.processUpdate();
	}

	private void processDelete() throws XMLStreamException, IOException {
		// 清理之前的状态
		this.delCmd.id = null;
		this.delCmd.query = null;
		this.delCmd.fromCommitted = true;
		this.delCmd.fromPending = true;

		for (int i = 0; i < this.reader.getAttributeCount(); i++) {
			String attrName = this.reader.getAttributeLocalName(i);
			String attrVal = this.reader.getAttributeValue(i);
			if ("fromPending".equals(attrName)) {
				this.delCmd.fromPending = StrUtils.parseBoolean(attrVal);
			} else if ("fromCommitted".equals(attrName)) {
				this.delCmd.fromCommitted = StrUtils.parseBoolean(attrVal);
			} else {
				logger.warn("unexpected attribute delete/@" + attrName);
			}
		}

		StringBuilder text = new StringBuilder();

		while (true) {
			int event = this.reader.next();
			switch (event) {
			case XMLStreamConstants.START_ELEMENT:
				String mode = this.reader.getLocalName();
				if (!("id".equals(mode) || "query".equals(mode))) {
					logger.warn("unexpected XML tag /delete/" + mode);
					throw new SolrException(
							SolrException.ErrorCode.BAD_REQUEST,
							"unexpected XML tag /delete/" + mode);
				}
				text.setLength(0);
				break;

			case XMLStreamConstants.END_ELEMENT:
				String currTag = this.reader.getLocalName();
				if ("id".equals(currTag)) {
					this.delCmd.id = text.toString();
				} else if ("query".equals(currTag)) {
					this.delCmd.query = text.toString();
				} else if ("delete".equals(currTag)) {
					return;
				} else {
					SolrXmlParseException e = new SolrXmlParseException("unexpected XML tag /delete/" + currTag);
					logger.warn("Bad xml format.", e);
					throw e;
				}
				//执行删除操作
				this.solrCore.getUpdateHandler().delete(this.delCmd);
				break;

			// Add everything to the text
			case XMLStreamConstants.SPACE:
			case XMLStreamConstants.CDATA:
			case XMLStreamConstants.CHARACTERS:
				text.append(this.reader.getText());
				break;
			}
		}
	}

	private void processUpdate() throws XMLStreamException, IOException {
		logger.warn("开始处理一次写索引请求。");
		while (true) {
			int event = this.reader.next();
			switch (event) {
			case XMLStreamConstants.END_DOCUMENT:
				return;
			case XMLStreamConstants.START_ELEMENT:
				String currTag = this.reader.getLocalName();
				if ("add".equals(currTag)) {
					boolean overwrite = true; // the default
					int commitWithin = -1;
					for (int i = 0; i < this.reader.getAttributeCount(); i++) {
						String attrName = this.reader.getAttributeLocalName(i);
						String attrVal = this.reader.getAttributeValue(i);
						if ("overwrite".equals(attrName)) {
							overwrite = Boolean.parseBoolean(attrVal);
						} else if ("commitWithin".equals(attrName)) {
							commitWithin = Integer.parseInt(attrVal);
						}
					}
					this.addCmd.overwriteCommitted = overwrite;
					this.addCmd.overwritePending = overwrite;
					this.addCmd.commitWithin = commitWithin;
					this.addCmd.allowDups = !overwrite;
				} else if ("doc".equals(currTag)) {
					this.addCmd.clear();
					addCmd.solrDoc = this.readDoc();
					try{
						addCmd.doc = this.getLuceneDocument(addCmd.solrDoc,this.schema);
					}catch(Exception e){
						logger.error("生成Docuemnt对象失败,忽略此条数据------继续 ==> " + addCmd.solrDoc,e);
						continue;
					}
					//添加索引
					this.solrCore.getUpdateHandler().addDoc(this.addCmd);
				} else if ("delete".equals(currTag)) {
					this.processDelete();
				}
				break;
			}
		}
	}

	private SolrInputDocument readDoc() throws XMLStreamException {
		this.solrDoc.clear();

		String attrName = "";
		for (int i = 0; i < this.reader.getAttributeCount(); i++) {
			attrName = this.reader.getAttributeLocalName(i);
			if ("boost".equals(attrName)) {
				this.solrDoc.setDocumentBoost(Float.parseFloat(reader
						.getAttributeValue(i)));
			} else {
				logger.warn("Unknown attribute doc/@" + attrName);
			}
		}

		StringBuilder text = new StringBuilder();
		String name = null;
		float boost = 1.0f;
		boolean isNull = false;

		while (true) {
			int event = this.reader.next();
			switch (event) {
			// Add everything to the text
			case XMLStreamConstants.SPACE:
			case XMLStreamConstants.CDATA:
			case XMLStreamConstants.CHARACTERS:
				text.append(this.reader.getText());
				break;
			case XMLStreamConstants.END_ELEMENT:
				if ("doc".equals(this.reader.getLocalName())) {
					return this.solrDoc;
				} else if ("field".equals(this.reader.getLocalName())) {
					if (!isNull) {
						this.solrDoc.addField(name, text.toString(), boost);
						boost = 1.0f;
					}
				}
				break;

			case XMLStreamConstants.START_ELEMENT:
				text.setLength(0);
				String localName = this.reader.getLocalName();
				if (!"field".equals(localName)) {
					SolrXmlParseException e = new SolrXmlParseException(
							"unexpected XML tag doc/" + localName);
					logger.error("Bad XML format.", e);
					throw e;
				}
				boost = 1.0f;
				String attrVal = "";
				for (int i = 0; i < this.reader.getAttributeCount(); i++) {
					attrName = this.reader.getAttributeLocalName(i);
					attrVal = this.reader.getAttributeValue(i);
					if ("name".equals(attrName)) {
						name = attrVal;
					} else if ("boost".equals(attrName)) {
						boost = Float.parseFloat(attrVal);
					} else if ("null".equals(attrName)) {
						isNull = Boolean.parseBoolean(attrVal);
					} else {
						logger.warn("Unknown attribute doc/field/@" + attrName);
					}
				}
				break;
			}
		}
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
	 * TODO: /!\ NOTE /!\ This semantics of this function are still in flux.
	 * Something somewhere needs to be able to fill up a SolrDocument from a
	 * lucene document - this is one place that may happen. It may also be moved
	 * to an independent function
	 * 
	 * @since solr 1.3
	 */
	private Document getLuceneDocument(SolrInputDocument doc, IndexSchema schema) {
		this.luceneDoc.getFields().clear();

		this.luceneDoc.setBoost(doc.getDocumentBoost());
		// Load fields from SolrDocument to Document
		for (SolrInputField field : doc) {
			String name = field.getName();
			SchemaField sfield = schema.getFieldOrNull(name);
			boolean used = false;
			float boost = field.getBoost();

			// Make sure it has the correct number
			if (sfield != null && !sfield.multiValued()
					&& field.getValueCount() > 1) {
				String id = "";
				SchemaField sf = schema.getUniqueKeyField();
				if (sf != null) {
					id = "[" + doc.getFieldValue(sf.getName()) + "] ";
				}

				SolrXmlParseException e = new SolrXmlParseException(
						id
								+ "multiple values encountered for non multiValued field "
								+ sfield.getName() + ": " + field.getValue());
				logger.error("Bad Solr xml format.", e);
				throw e;
			}

			// load each field value
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
						this.luceneDoc.add(f);
					used = true;
				} else {
					// TODO!!! HACK -- date conversion
					if (sfield != null && v instanceof Date
							&& sfield.getType() instanceof DateField) {
						DateField df = (DateField) sfield.getType();
						val = df.toInternal((Date) v) + 'Z';
					} else if (v != null) {
						val = v.toString();
					}

					if (sfield != null) {
						used = true;
						Field f = sfield.createField(val, boost);
						if (f != null) { // null fields are not added
							this.luceneDoc.add(f);
						}
					}
				}

				// Check if we should copy this field to any other fields.
				// This could happen whether it is explicit or not.
				List<CopyField> copyFields = schema.getCopyFieldsList(name);
				for (CopyField cf : copyFields) {
					SchemaField destinationField = cf.getDestination();
					// check if the copy field is a multivalued or not
					if (!destinationField.multiValued()
							&& this.luceneDoc.get(destinationField.getName()) != null) {
						SolrXmlParseException e = new SolrXmlParseException(
								"multiple values encountered for non multiValued copy field "
										+ destinationField.getName() + ": "
										+ val);
						logger.error("Bad xml format.", e);
						throw e;
					}

					used = true;
					Field f = null;
					if (isBinaryField) {
						if (destinationField.getType() instanceof BinaryField) {
							BinaryField binaryField = (BinaryField) destinationField
									.getType();
							binaryField.createField(destinationField, v, boost);
						}
					} else {
						f = destinationField.createField(cf
								.getLimitedValue(val), boost);
					}
					if (f != null) { // null fields are not added
						this.luceneDoc.add(f);
					}
				}

				// In lucene, the boost for a given field is the product of the
				// document boost and *all* boosts on values of that field.
				// For multi-valued fields, we only want to set the boost on the
				// first field.
				boost = 1.0f;
			}

			// make sure the field was used somehow...
			if (!used && hasField) {
				SolrXmlParseException e = new SolrXmlParseException(
						"ERROR:unknown field '" + name + "'");
				logger.error("bad xml format.", e);
				throw e;
			}
		}

		// Now validate required fields or add default values
		// fields with default values are defacto 'required'
		for (SchemaField field : schema.getRequiredFields()) {
			if (this.luceneDoc.getField(field.getName()) == null) {
				if (field.getDefaultValue() != null) {
					this.luceneDoc.add(field.createField(field
							.getDefaultValue(), 1.0f));
				} else {
					String id = schema.printableUniqueKey(this.luceneDoc);
					String msg = "Document [" + id
							+ "] missing required field: " + field.getName();
					SolrXmlParseException e = new SolrXmlParseException(msg);
					logger.error("bad xml format.", e);
					throw e;
				}
			}
		}

		return this.luceneDoc;
	}

	public void setReader(XMLStreamReader reader) {
		this.reader = reader;
	}

	public void setSchema(IndexSchema schema) {
		this.schema = schema;
	}

	public void setSolrCore(SolrCore solrCore) {
		this.solrCore = solrCore;
	}
}
