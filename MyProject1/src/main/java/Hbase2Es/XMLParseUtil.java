package Hbase2Es;

import org.apache.hadoop.hbase.util.Pair;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XMLParseUtil {

    private static Document parse(InputStream is) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        try {
            return factory.newDocumentBuilder().parse(is);
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        }

        return null;
    }

    public Map<String, Pair<String, String>> read(InputStream is) {
        Document document = parse(is);
        Element indexEl = document.getDocumentElement();
        Map<String, Pair<String, String>> map = new HashMap<>();
        List<Element> fieldEls = evalXPathAsElementList("field", indexEl);
        for (Element fieldEl : fieldEls) {
            String name = getAttribute(fieldEl, "name", true);
            String value = getAttribute(fieldEl, "value", true);
            String type = getAttribute(fieldEl, "type", false);
            if (type == null || type.equals("")) {
                type = "String";
            }
            Pair<String, String> val = new Pair(name, type);
            map.put(value, val);
        }
        return map;
    }

    private List<Element> evalXPathAsElementList(String expression, Node node) {
        try {
            XPathExpression expr = XPathFactory.newInstance().newXPath().compile(expression);
            NodeList list = (NodeList) expr.evaluate(node, XPathConstants.NODESET);
            List<Element> newList = new ArrayList<Element>(list.getLength());
            for (int i = 0; i < list.getLength(); i++) {
                newList.add((Element) list.item(i));
            }
            return newList;
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static String getAttribute(Element element, String name, boolean required) {
        /*if (!element.hasAttribute(name)) {
            if (required) {
                throw new IndexerConfException("Missing attribute " + name + " on element " + element.getLocalName());
            } else {
                return null;
            }
        }*/

        return element.getAttribute(name);
    }

    private <T extends Enum> T getEnumAttribute(Class<T> enumClass, Element element, String attribute, T defaultValue) {
        if (!element.hasAttribute(attribute)) {
            return defaultValue;
        }
        String value = element.getAttribute(attribute);
        try {
            return (T) Enum.valueOf(enumClass, value.toUpperCase());
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

        return null;
    }
}
