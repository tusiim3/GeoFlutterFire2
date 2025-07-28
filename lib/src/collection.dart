import 'dart:async';

import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:rxdart/rxdart.dart';

import 'models/DistanceDocSnapshot.dart';
import 'point.dart';
import 'util.dart';

class GeoFireCollectionRef {
  final Query<Map<String, dynamic>> _collectionReference;
  late final Stream<QuerySnapshot<Map<String, dynamic>>> _stream;

  GeoFireCollectionRef(this._collectionReference) {
    _stream = _createStream(_collectionReference)!.shareReplay(maxSize: 1);
  }

  /// return QuerySnapshot stream
  Stream<QuerySnapshot<Map<String, dynamic>>> snapshot() {
    return _stream;
  }

  /// return the Document mapped to the [id]
  Stream<List<DocumentSnapshot<Map<String, dynamic>>>> data(String id) {
    return _stream.map((querySnapshot) {
      return querySnapshot.docs
          .where((doc) => doc.id == id)
          .toList();
    });
  }

  /// add a document to collection with [data]
  Future<DocumentReference<Map<String, dynamic>>> add(Map<String, dynamic> data) {
    try {
      final colRef = _collectionReference as CollectionReference<Map<String, dynamic>>;
      return colRef.add(data);
    } catch (e) {
      throw Exception('cannot call add on Query, use collection reference instead');
    }
  }

  /// delete document with [id] from the collection
  Future<void> delete(String id) {
    try {
      final colRef = _collectionReference as CollectionReference<Map<String, dynamic>>;
      return colRef.doc(id).delete();
    } catch (e) {
      throw Exception('cannot call delete on Query, use collection reference instead');
    }
  }

  /// create or update a document with [id], [merge] defines whether the document should overwrite
  Future<void> setDoc(String id, Map<String, dynamic> data, {bool merge = false}) {
    try {
      final colRef = _collectionReference as CollectionReference<Map<String, dynamic>>;
      return colRef.doc(id).set(data, SetOptions(merge: merge));
    } catch (e) {
      throw Exception('cannot call set on Query, use collection reference instead');
    }
  }

  /// set a geo point with [latitude] and [longitude] using [field]
  Future<void> setPoint(String id, String field, double latitude, double longitude) {
    try {
      final colRef = _collectionReference as CollectionReference<Map<String, dynamic>>;
      final point = GeoFirePoint(latitude, longitude).data;
      return colRef.doc(id).set({'$field': point}, SetOptions(merge: true));
    } catch (e) {
      throw Exception('cannot call set on Query, use collection reference instead');
    }
  }

  /// Build combined stream for geohash area
  Stream<List<DocumentSnapshot<Map<String, dynamic>>>> _buildQueryStream({
    required GeoFirePoint center,
    required double radius,
    required String field,
    bool strictMode = false,
  }) {
    final precision = Util.setPrecision(radius);
    final centerHash = center.hash.substring(0, precision);
    final area = Set<String>.from(
      GeoFirePoint.neighborsOf(hash: centerHash)..add(centerHash),
    ).toList();

    final queries = area.map((hash) {
      final tempQuery = _queryPoint(hash, field);
      return _createStream(tempQuery)!.map((querySnapshot) {
        return querySnapshot.docs
            .map((element) => DistanceDocSnapshot(element, null))
            .toList();
      });
    });

    final mergedObservable = mergeObservable(queries);

    return mergedObservable.map((list) {
      final mappedList = list.map((distanceDocSnapshot) {
        final fieldList = field.split('.');
        final snapData = distanceDocSnapshot.documentSnapshot.data();
        var geoPointField = snapData[fieldList[0]];
        for (int i = 1; i < fieldList.length; i++) {
          geoPointField = geoPointField[fieldList[i]];
        }
        final GeoPoint geoPoint = geoPointField['geopoint'];
        distanceDocSnapshot.distance =
            center.distance(lat: geoPoint.latitude, lng: geoPoint.longitude);
        return distanceDocSnapshot;
      });

      final filteredList = strictMode
          ? mappedList
              .where((doc) => doc.distance! <= radius * 1.02)
              .toList()
          : mappedList.toList();

      filteredList.sort((a, b) => (a.distance! * 1000).toInt() - (b.distance! * 1000).toInt());

      return filteredList.map((e) => e.documentSnapshot).toList();
    });
  }

  Stream<List<DocumentSnapshot<Map<String, dynamic>>>> withinAsSingleStreamSubscription({
    required GeoFirePoint center,
    required double radius,
    required String field,
    bool strictMode = false,
  }) {
    return _buildQueryStream(center: center, radius: radius, field: field, strictMode: strictMode);
  }

  Stream<List<DocumentSnapshot<Map<String, dynamic>>>> within({
    required GeoFirePoint center,
    required double radius,
    required String field,
    bool strictMode = false,
  }) {
    return _buildQueryStream(center: center, radius: radius, field: field, strictMode: strictMode)
        .asBroadcastStream();
  }

  Stream<List<DistanceDocSnapshot>> mergeObservable(
      Iterable<Stream<List<DistanceDocSnapshot>>> queries) {
    return Rx.combineLatest(queries, (List<List<DistanceDocSnapshot>> lists) {
      return lists.expand((x) => x).toList();
    });
  }

  /// construct a query for the [geoHash] and [field]
  Query<Map<String, dynamic>> _queryPoint(String geoHash, String field) {
    final end = '$geoHash~';
    return _collectionReference
        .orderBy('$field.geohash')
        .startAt([geoHash])
        .endAt([end]);
  }

  Stream<QuerySnapshot<Map<String, dynamic>>>? _createStream(
      Query<Map<String, dynamic>> ref) {
    return ref.snapshots();
  }
}

