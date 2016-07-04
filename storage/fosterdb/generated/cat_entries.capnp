@0xfd9f583126433fe6;
struct FosterTableInfo {
  tablename @0 :Text;
  dbname @1 :Text;
  indexes @2 :List(FosterIndexInfo);
  foreignkeys @3 :List(FosterForeignTable);
  referencingKeys @4 :List(FosterReferencingTable);
}

struct FosterIndexInfo {
    indexname @0 :Text;
    stid @1 :UInt32;
    keylength @3 :UInt32;
    primary @4 :UInt32;
    partinfo @2 :List(FosterFieldInfo); 
}

struct FosterForeignTable{
    foreignTableId @0 :Text;
    foreignIdx @1 :Text;
    id @2:Text;
    foreignIdxPos @3 :UInt32;
    referencingIdx @4 :Text;
    referencingIdxPos @5 :UInt32;
    referencingStid @6 :UInt32;
}

struct FosterFieldInfo {
    fieldname @0 :Text;
    offset @1 :UInt32;
     length @2 :UInt32;
     nullBit @3 :UInt8;
     nullOffset @4 :UInt32;
     blob @5 :Bool;
     varlength @6 :Bool;
}

struct FosterReferencingTable{
  referencingTable @0 :Text;
  referencingIdx @1 :Text;
  referencingIdxPos @5 :UInt32;

  foreignIdx @6 :Text;
  foreignIdxPos @7 :UInt32;
  type @2 :Type;
  id @4 :Text;

  enum Type{
    delete @0;
    update @1;
  }

  action @3 :Action;
   enum Action {
    cascade @0;
    restrict @1;
    noaction @2;
    setdefault @3;
    setnull @4;
   }
}

