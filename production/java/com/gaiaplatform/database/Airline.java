// automatically generated by the FlatBuffers compiler, do not modify

package com.gaiaplatform.database;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class Airline extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_1_11_1(); }
  public static Airline getRootAsAirline(ByteBuffer _bb) { return getRootAsAirline(_bb, new Airline()); }
  public static Airline getRootAsAirline(ByteBuffer _bb, Airline obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public Airline __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public long GaiaId() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0L; }
  public int alId() { int o = __offset(6); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String name() { int o = __offset(8); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer nameAsByteBuffer() { return __vector_as_bytebuffer(8, 1); }
  public ByteBuffer nameInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 8, 1); }
  public String alias() { int o = __offset(10); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer aliasAsByteBuffer() { return __vector_as_bytebuffer(10, 1); }
  public ByteBuffer aliasInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 10, 1); }
  public String iata() { int o = __offset(12); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer iataAsByteBuffer() { return __vector_as_bytebuffer(12, 1); }
  public ByteBuffer iataInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 12, 1); }
  public String icao() { int o = __offset(14); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer icaoAsByteBuffer() { return __vector_as_bytebuffer(14, 1); }
  public ByteBuffer icaoInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 14, 1); }
  public String callsign() { int o = __offset(16); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer callsignAsByteBuffer() { return __vector_as_bytebuffer(16, 1); }
  public ByteBuffer callsignInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 16, 1); }
  public String country() { int o = __offset(18); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer countryAsByteBuffer() { return __vector_as_bytebuffer(18, 1); }
  public ByteBuffer countryInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 18, 1); }
  public String active() { int o = __offset(20); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer activeAsByteBuffer() { return __vector_as_bytebuffer(20, 1); }
  public ByteBuffer activeInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 20, 1); }

  public static int createAirline(FlatBufferBuilder builder,
      long Gaia_id,
      int al_id,
      int nameOffset,
      int aliasOffset,
      int iataOffset,
      int icaoOffset,
      int callsignOffset,
      int countryOffset,
      int activeOffset) {
    builder.startTable(9);
    Airline.addGaiaId(builder, Gaia_id);
    Airline.addActive(builder, activeOffset);
    Airline.addCountry(builder, countryOffset);
    Airline.addCallsign(builder, callsignOffset);
    Airline.addIcao(builder, icaoOffset);
    Airline.addIata(builder, iataOffset);
    Airline.addAlias(builder, aliasOffset);
    Airline.addName(builder, nameOffset);
    Airline.addAlId(builder, al_id);
    return Airline.endAirline(builder);
  }

  public static void startAirline(FlatBufferBuilder builder) { builder.startTable(9); }
  public static void addGaiaId(FlatBufferBuilder builder, long GaiaId) { builder.addLong(0, GaiaId, 0L); }
  public static void addAlId(FlatBufferBuilder builder, int alId) { builder.addInt(1, alId, 0); }
  public static void addName(FlatBufferBuilder builder, int nameOffset) { builder.addOffset(2, nameOffset, 0); }
  public static void addAlias(FlatBufferBuilder builder, int aliasOffset) { builder.addOffset(3, aliasOffset, 0); }
  public static void addIata(FlatBufferBuilder builder, int iataOffset) { builder.addOffset(4, iataOffset, 0); }
  public static void addIcao(FlatBufferBuilder builder, int icaoOffset) { builder.addOffset(5, icaoOffset, 0); }
  public static void addCallsign(FlatBufferBuilder builder, int callsignOffset) { builder.addOffset(6, callsignOffset, 0); }
  public static void addCountry(FlatBufferBuilder builder, int countryOffset) { builder.addOffset(7, countryOffset, 0); }
  public static void addActive(FlatBufferBuilder builder, int activeOffset) { builder.addOffset(8, activeOffset, 0); }
  public static int endAirline(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public Airline get(int j) { return get(new Airline(), j); }
    public Airline get(Airline obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

