import { useEffect, useRef } from "react";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import type { Marker } from "./api";

// OpenStreetMap's tiles: the one server besides the Brokers that måqtt talks to, and only while
// a map is shown. Their policy asks for this credit; it's plain text, as a link would open
// inside the app.
const TILES = "https://tile.openstreetmap.org/{z}/{x}/{y}.png";
const CREDIT = "© OpenStreetMap contributors";

/**
 * `to geo`'s markers on an OpenStreetMap map: where each key is, and a short trail. The view
 * fits the markers when `source` changes (Preview or the running Pipeline) and when a new key
 * appears; otherwise it's left where the user put it.
 */
export default function MarkerMap({
  markers,
  source,
}: {
  markers: Marker[];
  source: string;
}) {
  const element = useRef<HTMLDivElement>(null);
  const map = useRef<L.Map | null>(null);
  const layer = useRef<L.LayerGroup | null>(null);
  // How many markers the view was last fitted to, and for which source.
  const fitted = useRef<{ source: string; count: number } | null>(null);

  useEffect(() => {
    if (!element.current) return;
    const created = L.map(element.current, { worldCopyJump: true }).setView(
      [20, 0],
      2,
    );
    L.tileLayer(TILES, {
      maxZoom: 19,
      attribution: CREDIT,
      referrerPolicy: "strict-origin-when-cross-origin",
    }).addTo(created);
    created.attributionControl.setPrefix(false);
    layer.current = L.layerGroup().addTo(created);
    map.current = created;
    // The panel changes size with the window and the editor split.
    const resized = new ResizeObserver(() => created.invalidateSize());
    resized.observe(element.current);
    return () => {
      resized.disconnect();
      created.remove();
      map.current = null;
      layer.current = null;
      fitted.current = null;
    };
  }, []);

  useEffect(() => {
    const group = layer.current;
    if (!group || !map.current) return;
    group.clearLayers();
    for (const marker of markers) {
      const here: L.LatLngTuple = [marker.lat, marker.lon];
      if (marker.trail.length > 0) {
        L.polyline([...marker.trail, here], {
          className: "marker-map__trail",
          interactive: false,
        }).addTo(group);
      }
      L.circleMarker(here, { radius: 6, className: "marker-map__marker" })
        .bindTooltip(marker.label ?? marker.key, { direction: "top" })
        .addTo(group);
    }
    const last = fitted.current;
    const refit =
      markers.length > 0 &&
      (last === null || last.source !== source || markers.length > last.count);
    if (refit) {
      const bounds = L.latLngBounds(
        markers.flatMap((m) => [...m.trail, [m.lat, m.lon] as L.LatLngTuple]),
      );
      map.current.fitBounds(bounds, { padding: [40, 40], maxZoom: 14 });
      fitted.current = { source, count: markers.length };
    }
  }, [markers, source]);

  return (
    <div className="marker-map">
      <div ref={element} className="marker-map__canvas" />
      {markers.length === 0 && (
        <p className="marker-map__empty">
          No points yet. <code>to geo</code> places one for each value shaped
          like <code>{"{ lat, lon, label }"}</code>.
        </p>
      )}
    </div>
  );
}
