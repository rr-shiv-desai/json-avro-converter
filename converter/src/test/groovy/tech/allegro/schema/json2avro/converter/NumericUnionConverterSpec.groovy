package tech.allegro.schema.json2avro.converter

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import spock.lang.Unroll

class NumericUnionConverterSpec extends BaseConverterSpec {

    def "should convert union with int, long, and double - preserving long values"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "value",
                    "type" : ["null", "string", "int", "boolean", "long", "double"]
                  }
              ]
            }
        '''

        def json = '''
        {
            "value": 9999999999
        }
        '''

        when:
        GenericData.Record record = avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        record.get("value") instanceof Long
        record.get("value") == 9999999999L
    }

    def "should convert union with int, long, and double - preserving double values"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "value",
                    "type" : ["null", "string", "int", "boolean", "long", "double"]
                  }
              ]
            }
        '''

        def json = '''
        {
            "value": 123.456
        }
        '''

        when:
        GenericData.Record record = avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        record.get("value") instanceof Double
        record.get("value") == 123.456d
    }

    def "should convert union with int, long, and double - using int for small integers"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "value",
                    "type" : ["null", "string", "int", "boolean", "long", "double"]
                  }
              ]
            }
        '''

        def json = '''
        {
            "value": 42
        }
        '''

        when:
        GenericData.Record record = avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        record.get("value") instanceof Integer
        record.get("value") == 42
    }

    @Unroll
    def "should handle nested map with union values containing numeric types: #testCase"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "data",
                    "type" : [
                        "null",
                        {
                            "type": "map",
                            "values": {
                                "type": "map",
                                "values": ["null", "string", "int", "boolean", "long", "double"]
                            }
                        }
                    ]
                  }
              ]
            }
        '''

        when:
        GenericData.Record record = avroConverter.convertToGenericDataRecord(json, new Schema.Parser().parse(schema))
        def integrationData = record.get("data")
        def nestedMap = integrationData?.get(outerKey)
        def actualValue = nestedMap?.get(innerKey)

        then:
        actualValue != null
        actualValue.class == expectedType
        actualValue == expectedValue

        where:
        testCase           | json                                                                                      | outerKey    | innerKey  | expectedType    | expectedValue
        "long value"       | '{"data": {"provider": {"user_id": 9999999999}}}'.bytes                      | "provider"  | "user_id" | Long.class      | 9999999999L
        "double value"     | '{"data": {"provider": {"score": 98.765}}}'.bytes                            | "provider"  | "score"   | Double.class    | 98.765d
        "int value"        | '{"data": {"provider": {"count": 42}}}'.bytes                                | "provider"  | "count"   | Integer.class   | 42
        "string value"     | '{"data": {"provider": {"name": "test"}}}'.bytes                             | "provider"  | "name"    | String.class    | "test"
        "boolean value"    | '{"data": {"provider": {"active": true}}}'.bytes                             | "provider"  | "active"  | Boolean.class   | true
        "small decimal"    | '{"data": {"provider": {"rate": 1.5}}}'.bytes                                | "provider"  | "rate"    | Double.class    | 1.5d
        "negative long"    | '{"data": {"provider": {"offset": -9999999999}}}'.bytes                     | "provider"  | "offset"  | Long.class      | -9999999999L
        "zero value"       | '{"data": {"provider": {"counter": 0}}}'.bytes                               | "provider"  | "counter" | Integer.class   | 0
    }

    def "should handle round trip conversion for map with union numeric values"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "data",
                    "type" : {
                        "type": "map",
                        "values": ["null", "int", "long", "double"]
                    }
                  }
              ]
            }
        '''

        def json = '''
        {
            "data": {
                "int_val": 42,
                "long_val": 9999999999,
                "double_val": 123.456
            }
        }
        '''

        when:
        byte[] avro = avroConverter.convertToAvro(json.bytes, schema)
        def result = toMap(jsonConverter.convertToJson(avro, schema))

        then:
        result.data.int_val == 42
        result.data.long_val == 9999999999L
        result.data.double_val == 123.456d
    }

    def "should prefer int over long when value fits in int range"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "value",
                    "type" : ["int", "long"]
                  }
              ]
            }
        '''

        def json = '''
        {
            "value": 12345
        }
        '''

        when:
        GenericData.Record record = avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        record.get("value") instanceof Integer
        record.get("value") == 12345
    }

    def "should use long when value exceeds int range"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "value",
                    "type" : ["int", "long"]
                  }
              ]
            }
        '''

        def json = '''
        {
            "value": 2147483648
        }
        '''

        when:
        GenericData.Record record = avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        record.get("value") instanceof Long
        record.get("value") == 2147483648L
    }

    def "should handle max int value correctly"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "value",
                    "type" : ["int", "long"]
                  }
              ]
            }
        '''

        def json = """
        {
            "value": ${Integer.MAX_VALUE}
        }
        """

        when:
        GenericData.Record record = avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        record.get("value") instanceof Integer
        record.get("value") == Integer.MAX_VALUE
    }

    def "should handle min int value correctly"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "value",
                    "type" : ["int", "long"]
                  }
              ]
            }
        '''

        def json = """
        {
            "value": ${Integer.MIN_VALUE}
        }
        """

        when:
        GenericData.Record record = avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        record.get("value") instanceof Integer
        record.get("value") == Integer.MIN_VALUE
    }

    def "should use double for floating point even when union has int first"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "value",
                    "type" : ["int", "double"]
                  }
              ]
            }
        '''

        def json = '''
        {
            "value": 3.14159
        }
        '''

        when:
        GenericData.Record record = avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        record.get("value") instanceof Double
        record.get("value") == 3.14159d
    }
}
