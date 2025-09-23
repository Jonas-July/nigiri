#include <cassert>
#include <chrono>

#include "nigiri/loader/load.h"

#include "fmt/std.h"

#include "utl/enumerate.h"
#include "utl/get_or_create.h"
#include "utl/progress_tracker.h"

#include "nigiri/loader/dir.h"
#include "nigiri/loader/gtfs/loader.h"
#include "nigiri/loader/hrd/loader.h"
#include "nigiri/loader/init_finish.h"
#include "nigiri/loader/netex/loader.h"
#include "nigiri/shapes_storage.h"
#include "nigiri/timetable.h"
#include "nigiri/types.h"

namespace fs = std::filesystem;

namespace nigiri::loader {

std::vector<std::unique_ptr<loader_interface>> get_loaders() {
  auto loaders = std::vector<std::unique_ptr<loader_interface>>{};
  loaders.emplace_back(std::make_unique<gtfs::gtfs_loader>());
  loaders.emplace_back(std::make_unique<hrd::hrd_5_00_8_loader>());
  loaders.emplace_back(std::make_unique<hrd::hrd_5_20_26_loader>());
  loaders.emplace_back(std::make_unique<hrd::hrd_5_20_39_loader>());
  loaders.emplace_back(std::make_unique<hrd::hrd_5_20_avv_loader>());
  loaders.emplace_back(std::make_unique<netex::netex_loader>());
  return loaders;
}

std::pair<timetable, std::unique_ptr<shapes_storage>> load_from_source(uint64_t const idx,
                           dir* const dir,
                           assistance_times* a,
                           auto const it,
                           std::vector<timetable_source> const& sources,
                           interval<date::sys_days> const& date_range,
                           fs::path const cache_path,
                           shapes_storage const *  shapes) {
  // create local state
  auto const& [tag, path, local_config] = sources[idx];
  auto const load_local_cache_path = cache_path / fmt::format("tt{:d}", idx + sources.size());
  auto bitfields = hash_map<bitfield, bitfield_idx_t>{};
  auto shape_store = shapes != nullptr ? std::make_unique<shapes_storage>(load_local_cache_path, shapes->mode_) : nullptr;
  auto tt = timetable{};
  tt.date_range_ = date_range;
  tt.n_sources_ = static_cast<cista::base_t<source_idx_t>>(sources.size());
  /* Load file */
  try {
    (*it)->load(local_config, source_idx_t{0}, *dir, tt, bitfields, a, shape_store.get());
  } catch (std::exception const& e) {
    throw utl::fail("failed to load {}: {}", path, e.what());
  }
  tt.write(load_local_cache_path / "tt.bin");
  return std::make_pair(tt, std::move(shape_store));
}

using last_write_time_t = cista::strong<std::int64_t, struct _last_write_time>;
using source_path_t = cista::basic_string<char const*>;

struct change_detector {
  vector_map<source_idx_t, source_path_t> source_paths;
  vector_map<source_idx_t, last_write_time_t> last_write_times;
};

struct index_mapping {
  alt_name_idx_t alt_name_idx_offset;
  area_idx_t area_idx_offset;
  booking_rule_idx_t booking_rule_idx_offset;
  flex_stop_seq_idx_t flex_stop_seq_idx_offset;
  flex_transport_idx_t flex_transport_idx_offset;
  language_idx_t language_idx_offset;
  location_group_idx_t location_group_idx_offset;
  location_idx_t location_idx_offset;
  merged_trips_idx_t merged_trips_idx_offset;
  provider_idx_t provider_idx_offset;
  route_idx_t route_idx_offset;
  source_file_idx_t source_file_idx_offset;
  source_idx_t source_idx_offset;
  timezone_idx_t timezone_idx_offset;
  transport_idx_t transport_idx_offset;
  trip_direction_string_idx_t trip_direction_string_idx_offset;
  trip_id_idx_t trip_id_idx_offset;
  trip_idx_t trip_idx_offset;

  index_mapping(timetable first_tt, source_idx_t src)
    : alt_name_idx_offset{first_tt.locations_.alt_name_strings_.size()},
      area_idx_offset{first_tt.areas_.size()},
      booking_rule_idx_offset{first_tt.booking_rules_.size()},
      flex_stop_seq_idx_offset{first_tt.flex_stop_seq_.size()},
      flex_transport_idx_offset{first_tt.flex_transport_traffic_days_.size()},
      language_idx_offset{first_tt.languages_.size()},
      location_group_idx_offset{first_tt.location_group_name_.size()},
      location_idx_offset{first_tt.n_locations()},
      merged_trips_idx_offset{first_tt.merged_trips_.size()},
      provider_idx_offset{first_tt.providers_.size()},
      route_idx_offset{first_tt.n_routes()},
      source_file_idx_offset{first_tt.source_file_names_.size()},
      source_idx_offset{src},
      timezone_idx_offset{first_tt.locations_.timezones_.size()},
      transport_idx_offset{first_tt.transport_traffic_days_.size()},
      trip_direction_string_idx_offset{first_tt.trip_direction_strings_.size()},
      trip_id_idx_offset{first_tt.trip_id_strings_.size()},
      trip_idx_offset{first_tt.trip_ids_.size()} {}

  auto map(alt_name_idx_t i) { return i + alt_name_idx_offset; }
  auto map(area_idx_t i) { return i + area_idx_offset; }
  auto map(booking_rule_idx_t i) { return i + booking_rule_idx_offset; }
  auto map(flex_stop_seq_idx_t i) { return i + flex_stop_seq_idx_offset; }
  auto map(flex_transport_idx_t i) { return i + flex_transport_idx_offset; }
  auto map(language_idx_t i) { return i + language_idx_offset; }
  auto map(location_group_idx_t i) { return i + location_group_idx_offset; }
  auto map(location_idx_t i) { return i + location_idx_offset; }
  auto map(merged_trips_idx_t i) { return i + merged_trips_idx_offset; }
  auto map(provider_idx_t i) { return i + provider_idx_offset; }
  auto map(route_idx_t i) { return i + route_idx_offset; }
  auto map(source_file_idx_t i) { return i + source_file_idx_offset; }
  auto map(source_idx_t i) { return i + source_idx_offset; }
  auto map(stop::value_type i) { return to_idx(map(location_idx_t{i})); }
  auto map(timezone_idx_t i) { return i + timezone_idx_offset; }
  auto map(transport_idx_t i) { return i + transport_idx_offset; }
  auto map(trip_debug i) { return trip_debug{map(i.source_file_idx_), i.line_number_from_, i.line_number_to_}; }
  auto map(trip_direction_string_idx_t i) { return i + trip_direction_string_idx_offset; }
  auto map(trip_direction_t i) { return i.apply([&](auto const& d) -> trip_direction_t { return trip_direction_t{map(d)}; });}
  auto map(trip_id_idx_t i) { return i + trip_id_idx_offset; }
  auto map(trip_idx_t i) { return i + trip_idx_offset; }

  auto map(fares::fare_leg_join_rule i) { return fares::fare_leg_join_rule{i.from_network_, i.to_network_, map(i.from_stop_), map(i.to_stop_)}; }
  auto map(fares::fare_leg_rule i) { return fares::fare_leg_rule{
                                       .rule_priority_ = i.rule_priority_,
                                       .network_ = i.network_,
                                       .from_area_ = map(i.from_area_),
                                       .to_area_ = map(i.to_area_),
                                       .from_timeframe_group_ = i.from_timeframe_group_,
                                       .to_timeframe_group_ = i.to_timeframe_group_,
                                       .fare_product_ = i.fare_product_,
                                       .leg_group_idx_ = i.leg_group_idx_,
                                       .contains_exactly_area_set_id_ = i.contains_exactly_area_set_id_,
                                       .contains_area_set_id_ = i.contains_area_set_id_};
                                   }
  auto map(footpath i) { return footpath{map(i.target()), i.duration()}; }
  auto map(location_id i) { return location_id{i.id_, map(i.src_)}; }

  template<typename T>
  auto map(interval<T> i) {
    return interval{map(i.from_), map(i.to_)};
  }

  template<typename T1, typename T2>
  auto map(pair<T1, T2> i) {
    return pair<T1, T2>{map(i.first), map(i.second)};
  }

  template<typename T>
  auto map(T i) {
    return i;
  }
};

timetable load(std::vector<timetable_source> const& sources,
               finalize_options const& finalize_opt,
               interval<date::sys_days> const& date_range,
               assistance_times* a,
               shapes_storage* shapes,
               bool ignore) {
  auto const loaders = get_loaders();
  auto cache_path = fs::path{"cache"};
  auto cache_metadata_path = cache_path / "meta.bin";

  fs::create_directories(cache_path);
  auto chg = change_detector{};
  for (auto const& in : sources) {
    auto const& [tag, path, local_config] = in;

    auto const last_write_time = fs::last_write_time(path);
    auto const timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
                     std::chrono::file_clock::to_sys(last_write_time).time_since_epoch()).count();
    chg.source_paths.emplace_back(path);
    chg.last_write_times.emplace_back(last_write_time_t{timestamp});
  }
  auto saved_changes = change_detector{};
  try {
    saved_changes = *cista::read<change_detector>(cache_metadata_path);
  } catch (std::exception const& e) {
    log(log_lvl::info, "loader.load", "no cache metadata at {} found", cache_metadata_path);
  }
  auto first_recomputed_source = source_idx_t{chg.source_paths.size()};
  for (auto const [idx, in] : utl::enumerate(chg.source_paths)) {
    auto const src = source_idx_t{idx};
    if (idx >= saved_changes.source_paths.size()) {
      first_recomputed_source = src;
      break;
    }
    if (in != saved_changes.source_paths[src] || chg.last_write_times[src] != saved_changes.last_write_times[src]) {
      first_recomputed_source = src;
      break;
    }
  }

  auto tt = timetable{};
  for (auto i = first_recomputed_source; i >= 0; --i ) {
    if (i == 0) {
      tt.date_range_ = date_range;
      tt.n_sources_ = static_cast<cista::base_t<source_idx_t>>(sources.size());
      register_special_stations(tt);
      break;
    }
    auto const prev = i - 1;
    auto const local_cache_path = cache_path / fmt::format("tt{:d}", to_idx(prev));
    auto cached_timetable = timetable{};
    try {
      cached_timetable = *cista::read<timetable>(local_cache_path / "tt.bin");
    } catch (std::exception const& e) {
      log(log_lvl::info, "loader.load", "no cached timetable at {} found", local_cache_path / "tt.bin");
      continue;
    }
    cached_timetable.resolve();
    if (cached_timetable.date_range_ != date_range) {
      continue;
    }
    first_recomputed_source = i;
    tt = cached_timetable;
    auto cached_shape_store = std::make_unique<shapes_storage>(local_cache_path, cista::mmap::protection::READ);
    shapes->add(cached_shape_store.get());
    break;
  }

  cista::write(cache_metadata_path, chg);

  for (auto const [idx, in] : utl::enumerate(sources)) {
    auto const local_cache_path = cache_path / fmt::format("tt{:d}", idx);
    auto const src = source_idx_t{idx};
    if (src < first_recomputed_source) {
      continue;
    }
    auto const& [tag, path, local_config] = in;
    auto const is_in_memory = path.starts_with("\n#");
    auto const dir = is_in_memory
                         // hack to load strings in integration tests
                         ? std::make_unique<mem_dir>(mem_dir::read(path))
                         : make_dir(path);
    auto const it =
        utl::find_if(loaders, [&](auto&& l) { return l->applicable(*dir); });
    if (it != end(loaders)) {
      if (!is_in_memory) {
        log(log_lvl::info, "loader.load", "loading {}", path);
      }
      auto const progress_tracker = utl::get_active_progress_tracker();
      progress_tracker->context(std::string{tag});
      progress_tracker->status("Merging...");
      /* Prepare timetable by emptying corrected fields */
      // Fields not used during loading
      assert(tt.locations_.footpaths_out_.size() == kNProfiles);
      for (auto i : tt.locations_.footpaths_out_) {
        assert(i.size() == 0);
      }
      assert(tt.locations_.footpaths_in_.size() == kNProfiles);
      for (auto i : tt.locations_.footpaths_in_) {
        assert(i.size() == 0);
      }
      assert(tt.fwd_search_lb_graph_.size() == kNProfiles);
      for (auto i : tt.fwd_search_lb_graph_) {
        assert(i.size() == 0);
      }
      assert(tt.bwd_search_lb_graph_.size() == kNProfiles);
      for (auto i : tt.bwd_search_lb_graph_) {
        assert(i.size() == 0);
      }
      assert(tt.flex_area_locations_.size() == 0);
      assert(tt.trip_train_nr_.size() == 0);
      assert(tt.initial_day_offset_.size() == 0);
      assert(tt.profiles_.size() == 0);
      assert(tt.date_range_ == date_range);
      auto result = load_from_source(idx, dir.get(), a, it, sources, date_range, cache_path, shapes);
      auto other_tt = result.first;
      auto shape_store = std::move(result.second);
      /* Save new data */
      auto new_bitfields = other_tt.bitfields_;
      auto new_source_end_date = other_tt.src_end_date_;
      auto new_trip_id_to_idx = other_tt.trip_id_to_idx_;
      auto new_trip_ids = other_tt.trip_ids_;
      auto new_trip_id_strings = other_tt.trip_id_strings_;
      auto new_trip_id_src = other_tt.trip_id_src_;
      auto new_trip_direction_id = other_tt.trip_direction_id_;
      auto new_trip_route_id = other_tt.trip_route_id_;
      auto new_route_ids = other_tt.route_ids_;
      auto new_trip_transport_ranges = other_tt.trip_transport_ranges_;
      auto new_trip_stop_seq_numbers = other_tt.trip_stop_seq_numbers_;
      auto new_source_file_names = other_tt.source_file_names_;
      auto new_trip_debug = other_tt.trip_debug_;
      auto new_trip_short_names = other_tt.trip_short_names_;
      auto new_trip_display_names = other_tt.trip_display_names_;
      auto new_route_transport_ranges = other_tt.route_transport_ranges_;
      auto new_route_location_seq = other_tt.route_location_seq_;
      auto new_route_clasz = other_tt.route_clasz_;
      auto new_route_section_clasz = other_tt.route_section_clasz_;
      auto new_route_bikes_allowed = other_tt.route_bikes_allowed_;
      auto new_route_cars_allowed = other_tt.route_cars_allowed_;
      auto new_route_bikes_allowed_per_section = other_tt.route_bikes_allowed_per_section_;
      auto new_route_cars_allowed_per_section = other_tt.route_cars_allowed_per_section_;
      auto new_route_stop_time_ranges = other_tt.route_stop_time_ranges_;
      auto new_route_stop_times = other_tt.route_stop_times_;
      auto new_transport_first_dep_offset = other_tt.transport_first_dep_offset_;
      auto new_transport_traffic_days = other_tt.transport_traffic_days_;
      auto new_transport_route = other_tt.transport_route_;
      auto new_transport_to_trip_section = other_tt.transport_to_trip_section_;
      auto new_languages = other_tt.languages_;
      auto new_locations = other_tt.locations_;
      auto new_merged_trips = other_tt.merged_trips_;
      auto new_attributes = other_tt.attributes_;
      auto new_attribute_combinations = other_tt.attribute_combinations_;
      auto new_trip_direction_strings = other_tt.trip_direction_strings_;
      auto new_trip_directions = other_tt.trip_directions_;
      auto new_trip_lines = other_tt.trip_lines_;
      auto new_transport_section_attributes = other_tt.transport_section_attributes_;
      auto new_transport_section_providers = other_tt.transport_section_providers_;
      auto new_transport_section_directions = other_tt.transport_section_directions_;
      auto new_transport_section_lines = other_tt.transport_section_lines_;
      auto new_transport_section_route_colors = other_tt.transport_section_route_colors_;
      auto new_location_routes = other_tt.location_routes_;
      auto new_providers = other_tt.providers_;
      auto new_provider_id_to_idx = other_tt.provider_id_to_idx_;
      auto new_fares = other_tt.fares_;
      auto new_areas = other_tt.areas_;
      auto new_location_areas = other_tt.location_areas_;
      auto new_location_location_groups = other_tt.location_location_groups_;
      auto new_location_group_locations = other_tt.location_group_locations_;
      auto new_location_group_name = other_tt.location_group_name_;
      auto new_location_group_id = other_tt.location_group_id_;
      auto new_flex_area_bbox = other_tt.flex_area_bbox_;
      auto new_flex_area_id = other_tt.flex_area_id_;
      auto new_flex_area_src = other_tt.flex_area_src_;
      auto new_flex_area_outers = other_tt.flex_area_outers_;
      auto new_flex_area_inners = other_tt.flex_area_inners_;
      auto new_flex_area_name = other_tt.flex_area_name_;
      auto new_flex_area_desc = other_tt.flex_area_desc_;
      auto new_flex_area_rtree = other_tt.flex_area_rtree_;
      auto new_location_group_transports = other_tt.location_group_transports_;
      auto new_flex_area_transports = other_tt.flex_area_transports_;
      auto new_flex_transport_traffic_days = other_tt.flex_transport_traffic_days_;
      auto new_flex_transport_trip = other_tt.flex_transport_trip_;
      auto new_flex_transport_stop_time_windows = other_tt.flex_transport_stop_time_windows_;
      auto new_flex_transport_stop_seq = other_tt.flex_transport_stop_seq_;
      auto new_flex_stop_seq = other_tt.flex_stop_seq_;
      auto new_flex_transport_pickup_booking_rule = other_tt.flex_transport_pickup_booking_rule_;
      auto new_flex_transport_drop_off_booking_rule = other_tt.flex_transport_drop_off_booking_rule_;
      auto new_booking_rules = other_tt.booking_rules_;
      auto new_strings = other_tt.strings_;
      progress_tracker->status("Saved new data");
      /* Add new data and adjust references */
      auto im = index_mapping(tt, src);
      /*	bitfields	*/
      auto corrected_indices = vector_map<bitfield_idx_t, bitfield_idx_t>{};
      auto bitfields_ = hash_map<bitfield, bitfield_idx_t>{};
      for (auto const [idx_, bf] : utl::enumerate(tt.bitfields_)) {
        auto new_idx = utl::get_or_create(
          bitfields_, bf, [&]() { return idx_; });
        assert(new_idx == idx_); // bitfields must be unique in the timetable
      }
      for (auto const& [idx_, bf] : utl::enumerate(new_bitfields)) {
        auto adjusted_idx = utl::get_or_create(
            bitfields_, bf, [&]() { return tt.register_bitfield(bf); });
        corrected_indices.emplace_back(adjusted_idx);
      }
      /*       string_idx_t	*/
      auto string_map = vector_map<string_idx_t, string_idx_t>{};
      for (auto const& [idx_, s] : utl::enumerate(new_strings.strings_)) {
        auto new_idx = tt.strings_.store(s.view());
        string_map.push_back(new_idx);
      }
      /*	 sources	*/
      for (auto i : new_source_end_date) {
        tt.src_end_date_.push_back(i);
      }
      for (auto i : new_source_file_names) {
        tt.source_file_names_.emplace_back(i);
      }
      for (auto i : new_trip_debug) {
        auto entry = tt.trip_debug_.emplace_back();
        for (auto j : i) {
          entry.emplace_back(im.map(j));
        }
      }
      /*	 languages	*/
      for (auto i : new_languages) {
        tt.languages_.emplace_back(i);
      }
      /*       location_idx_t	*/
      {// merge locations struct
        auto&& loc = tt.locations_;
        for (auto i : new_locations.location_id_to_idx_) {
          auto loc_id = im.map(i.first);
          auto loc_idx = im.map(i.second);
          auto const [it, is_new] = loc.location_id_to_idx_.emplace(loc_id, loc_idx);
          if (!is_new) {
            log(log_lvl::error, "loader.load", "duplicate station {}", loc_id.id_);
          }
        }
        for (auto i : new_locations.names_) {
          loc.names_.emplace_back(i);
        }
        for (auto i : new_locations.platform_codes_) {
          loc.platform_codes_.emplace_back(i);
        }
        for (auto i : new_locations.descriptions_) {
          loc.descriptions_.emplace_back(i);
        }
        for (auto i : new_locations.ids_) {
          loc.ids_.emplace_back(i);
        }
        for (auto i : new_locations.alt_names_) {
          auto vec = loc.alt_names_.add_back_sized(0U);
          for (auto j : i) {
            vec.push_back(im.map(j));
          }
        }
        for (auto i: new_locations.coordinates_) {
          loc.coordinates_.push_back(i);
        }
        for (auto i: new_locations.src_) {
          loc.src_.push_back(im.map(i));
        }
        for (auto i: new_locations.transfer_time_) {
          loc.transfer_time_.push_back(i);
        }
        for (auto i: new_locations.types_) {
          loc.types_.push_back(i);
        }
        for (auto i: new_locations.parents_) {
          loc.parents_.push_back(im.map(i));
        }
        for (auto i: new_locations.location_timezones_) {
          loc.location_timezones_.push_back(im.map(i));
        }
        for (auto i : new_locations.equivalences_) {
          auto entry = loc.equivalences_.emplace_back();
          for (auto j : i) {
            entry.emplace_back(im.map(j));
          }
        }
        for (auto i : new_locations.children_) {
          auto entry = loc.children_.emplace_back();
          for (auto j : i) {
            entry.emplace_back(im.map(j));
          }
        }
        for (auto i : new_locations.preprocessing_footpaths_out_) {
          auto entry = loc.preprocessing_footpaths_out_.emplace_back();
          for (auto j : i) {
            entry.emplace_back(im.map(j));
          }
        }
        for (auto i : new_locations.preprocessing_footpaths_in_) {
          auto entry = loc.preprocessing_footpaths_in_.emplace_back();
          for (auto j : i) {
            entry.emplace_back(im.map(j));
          }
        }
        /*
          loc.footpaths_out_ and loc.footpaths_in_ don't get used during loading
          and are thus skipped
        */
        assert(new_locations.footpaths_out_.size() == kNProfiles);
        for (auto i : new_locations.footpaths_out_) {
          assert(i.size() == 0);
        }
        assert(new_locations.footpaths_in_.size() == kNProfiles);
        for (auto i : new_locations.footpaths_in_) {
          assert(i.size() == 0);
        }
        for (auto i: new_locations.timezones_) {
          loc.timezones_.push_back(i);
        }
        /*
          loc.location_importance_ doesn't get used during loading and is thus skipped
        */
        assert(loc.location_importance_.size() == 0);
        for (auto i : new_locations.alt_name_strings_) {
          loc.alt_name_strings_.emplace_back(i);
        }
        for (auto i: new_locations.alt_name_langs_) {
          loc.alt_name_langs_.push_back(im.map(i));
        }
        /*
          loc.max_importance_ and loc.rtree_ don't get used during loading
          and are thus skipped
        */
        assert(loc.max_importance_ == 0U);
      } // end of locations struct
      for (auto i : new_location_routes) {
        auto vec = tt.location_routes_.add_back_sized(0U);
        for (auto j : i) {
          vec.push_back(im.map(j));
        }
      }
      for (auto i : new_location_areas) {
        auto vec = tt.location_areas_.add_back_sized(0U);
        for (auto j : i) {
          vec.push_back(im.map(j));
        }
      }
      for (location_idx_t i = location_idx_t{0}; i < location_idx_t{new_location_location_groups.size()}; ++i) {
        tt.location_location_groups_.emplace_back_empty();
        for (auto j : new_location_location_groups[i]) {
          tt.location_location_groups_.back().push_back(im.map(j));
        }
      }
      for (location_group_idx_t i = location_group_idx_t{0}; i < location_group_idx_t{new_location_group_locations.size()}; ++i) {
        tt.location_group_locations_.emplace_back_empty();
        for (auto j : new_location_group_locations[location_group_idx_t{i}]) {
          tt.location_group_locations_.back().push_back(im.map(j));
        }
      }
      //tt.fwd_search_lb_graph_ not used during loading
      assert(tt.fwd_search_lb_graph_.size() == kNProfiles);
      for (auto i : tt.fwd_search_lb_graph_) {
        assert(i.size() == 0);
      }
      //tt.bwd_search_lb_graph_ not used during loading
      assert(tt.bwd_search_lb_graph_.size() == kNProfiles);
      for (auto i : tt.bwd_search_lb_graph_) {
        assert(i.size() == 0);
      }
      /*        route_idx_t	*/
      for (auto i : new_route_transport_ranges) {
        tt.route_transport_ranges_.push_back(im.map(i));
      }
      for (auto i : new_route_location_seq) {
        auto vec = tt.route_location_seq_.add_back_sized(0U);
        for (auto j : i) {
          vec.push_back(im.map(j));
        }
      }
      for (auto i : new_route_clasz) {
        tt.route_clasz_.emplace_back(i);
      }
      for (auto i : new_route_section_clasz) {
        tt.route_section_clasz_.emplace_back(i);
      }
      for (auto i : new_route_bikes_allowed_per_section) {
        tt.route_bikes_allowed_per_section_.emplace_back(i);
      }
      for (auto i : new_route_cars_allowed_per_section) {
        tt.route_cars_allowed_per_section_.emplace_back(i);
      }
      auto route_bikes_allowed_size = tt.route_bikes_allowed_.size();
      tt.route_bikes_allowed_.resize(tt.route_bikes_allowed_.size() + new_route_bikes_allowed.size());
      for (auto i = 0U; i < new_route_bikes_allowed.size(); ++i) {
        tt.route_bikes_allowed_.set(route_bikes_allowed_size + i, new_route_bikes_allowed.test(i));
      }
      auto route_cars_allowed_size = tt.route_cars_allowed_.size();
      tt.route_cars_allowed_.resize(tt.route_cars_allowed_.size() + new_route_cars_allowed.size());
      for (auto i = 0U; i < new_route_cars_allowed.size(); ++i) {
        tt.route_cars_allowed_.set(route_cars_allowed_size + i, new_route_cars_allowed.test(i));
      }
      auto const route_stop_times_offset = tt.route_stop_times_.size();
      for (auto i : new_route_stop_time_ranges) {
        tt.route_stop_time_ranges_.push_back(interval{i.from_ + route_stop_times_offset, i.to_ + route_stop_times_offset});
      }
      for (auto i : new_route_stop_times) {
        tt.route_stop_times_.push_back(i);
      }
      for (auto i : new_transport_route) {
        tt.transport_route_.push_back(im.map(i));
      }
      /*          fares		*/
      for (auto i : new_fares) {
        auto new_leg_group_name = vector_map<leg_group_idx_t, string_idx_t>{};
        for (auto j : i.leg_group_name_) {
          new_leg_group_name.push_back(string_map[j]);
        }
        auto new_fare_media = vector_map<fare_media_idx_t, fares::fare_media>{};
        for (auto j : i.fare_media_) {
          j.name_ = string_map[j.name_];
          new_fare_media.push_back(j);
        }
        auto new_fare_products = vecvec<fare_product_idx_t, fares::fare_product>{};
        for (auto j : i.fare_products_) {
          auto vec = new_fare_products.add_back_sized(0U);
          for (auto k : j) {
            k.name_ = string_map[k.name_];
            k.currency_code_ = string_map[k.currency_code_];
            vec.push_back(k);
          }
        }
        auto new_fare_product_id = vector_map<fare_product_idx_t, string_idx_t>{};
        for (auto j : i.fare_product_id_) {
          new_fare_product_id.push_back(string_map[j]);
        }
        auto new_fare_leg_rules = vector<fares::fare_leg_rule>{};
        for (auto j : i.fare_leg_rules_) {
          new_fare_leg_rules.push_back(im.map(j));
        }
        auto new_fare_leg_join_rules = vector<fares::fare_leg_join_rule>{};
        for (auto j : i.fare_leg_join_rules_) {
          new_fare_leg_join_rules.push_back(im.map(j));
        }
        auto new_rider_categories = vector_map<rider_category_idx_t, fares::rider_category>{};
        for (auto j : i.rider_categories_) {
          j.name_ = string_map[j.name_];
          j.eligibility_url_ = string_map[j.eligibility_url_];
          new_rider_categories.push_back(j);
        }
        auto new_timeframes = vecvec<timeframe_group_idx_t, fares::timeframe>{};
        for (auto j : i.timeframes_) {
          auto vec = new_timeframes.add_back_sized(0U);
          for (auto k : j) {
            k.service_id_ = string_map[k.service_id_];
            vec.push_back(k);
          }
        }
        auto new_timeframe_id = vector_map<timeframe_group_idx_t, string_idx_t>{};
        for (auto j : i.timeframe_id_) {
          new_timeframe_id.push_back(string_map[j]);
        }
        auto new_networks = vector_map<network_idx_t, fares::network>{};
        for (auto j : i.networks_) {
          j.id_ = string_map[j.id_];
          j.name_ = string_map[j.name_];
          new_networks.push_back(j);
        }
        auto new_area_sets = vecvec<area_set_idx_t, area_idx_t>{};
        for (auto j : i.area_sets_) {
          auto vec = new_area_sets.add_back_sized(0U);
          for (auto k : j) {
             vec.push_back(im.map(k));
          }
        }
        auto new_area_set_ids = vector_map<area_set_idx_t, string_idx_t>{};
        for (auto j : i.area_set_ids_) {
          new_area_set_ids.push_back(string_map[j]);
        }
        i.leg_group_name_ = new_leg_group_name;
        i.fare_media_ = new_fare_media;
        i.fare_products_ = new_fare_products;
        i.fare_product_id_ = new_fare_product_id;
        i.fare_leg_rules_ = new_fare_leg_rules;
        i.fare_leg_join_rules_ = new_fare_leg_join_rules;
        i.rider_categories_ = new_rider_categories;
        i.timeframes_ = new_timeframes;
        i.timeframe_id_ = new_timeframe_id;
        i.networks_ = new_networks;
        i.area_sets_ = new_area_sets;
        i.area_set_ids_ = new_area_set_ids;
        tt.fares_.push_back(i);
      }
      /*      provider_idx_t	*/
      for (auto i : new_providers) {
        i.id_ = string_map[i.id_];
        i.name_ = string_map[i.name_];
        i.url_ = string_map[i.url_];
        i.tz_ = im.map(i.tz_);
        i.src_ = im.map(i.src_);
        tt.providers_.push_back(i);
      }
      for (auto i : new_provider_id_to_idx) {
        tt.provider_id_to_idx_.push_back(im.map(i));
      }
      /*	  Flex		*/
      for (auto i : new_flex_area_bbox) {
        tt.flex_area_bbox_.push_back(i);
      }
      for (auto i : new_flex_area_id) {
        tt.flex_area_id_.push_back(string_map[i]);
      }
      for (auto i : new_flex_area_src) {
        tt.flex_area_src_.push_back(im.map(i));
      }
      //tt.flex_area_locations_ not used during loading
      assert(tt.flex_area_locations_.size() == 0);
      for (auto i : new_flex_area_outers) {
        tt.flex_area_outers_.emplace_back(i);
      }
      for (auto i : new_flex_area_inners) {
        tt.flex_area_inners_.emplace_back(i);
      }
      for (auto i : new_flex_area_name) {
        tt.flex_area_name_.emplace_back(i);
      }
      for (auto i : new_flex_area_desc) {
        tt.flex_area_desc_.emplace_back(i);
      }
      for (auto n : new_flex_area_rtree.nodes_) {
        if (n.kind_ == rtree<flex_area_idx_t>::kind::kLeaf) {
          for (size_t i = 0; i < n.count_; ++i) {
            tt.flex_area_rtree_.insert(n.rects_[i].min_, n.rects_[i].max_, n.data_[i]);
          }
        }
      }
      for (location_group_idx_t i = location_group_idx_t{0}; i < location_group_idx_t{new_location_group_transports.size()}; ++i) {
        tt.location_group_transports_.emplace_back_empty();
        for (auto j : new_location_group_transports[i]) {
          tt.location_group_transports_.back().push_back(im.map(j));
        }
      }
      for (flex_area_idx_t i = flex_area_idx_t{0}; i < flex_area_idx_t{new_flex_area_transports.size()}; ++i) {
        tt.flex_area_transports_.emplace_back_empty();
        for (auto j : new_flex_area_transports[i]) {
          tt.flex_area_transports_.back().push_back(im.map(j));
        }
      }
      for (auto i : new_flex_transport_traffic_days) {
        tt.flex_transport_traffic_days_.push_back(corrected_indices[bitfield_idx_t{i}]);
      }
      for (auto i : new_flex_transport_trip) {
        tt.flex_transport_trip_.push_back(im.map(i));
      }
      for (auto i : new_flex_transport_stop_time_windows) {
        tt.flex_transport_stop_time_windows_.emplace_back(i);
      }
      for (auto i : new_flex_transport_stop_seq) {
        tt.flex_transport_stop_seq_.push_back(im.map(i));
      }
      for (auto i : new_flex_stop_seq) {
        tt.flex_stop_seq_.emplace_back(i);
      }
      for (auto i : new_flex_transport_pickup_booking_rule) {
        auto vec = tt.flex_transport_pickup_booking_rule_.add_back_sized(0U);
        for (auto j : i) {
          vec.push_back(im.map(j));
        }
      }
      for (auto i : new_flex_transport_drop_off_booking_rule) {
        auto vec = tt.flex_transport_drop_off_booking_rule_.add_back_sized(0U);
        for (auto j : i) {
          vec.push_back(im.map(j));
        }
      }
      for (auto i : new_booking_rules) {
        i.id_ = string_map[i.id_];
        i.message_ = string_map[i.message_];
        i.pickup_message_ = string_map[i.pickup_message_];
        i.drop_off_message_ = string_map[i.drop_off_message_];
        i.phone_number_ = string_map[i.phone_number_];
        i.info_url_ = string_map[i.info_url_];
        i.booking_url_ = string_map[i.booking_url_];
        tt.booking_rules_.push_back(i);
      }
      /*      trip_id_idx_t	*/
      for (auto i : new_trip_id_to_idx) {
        tt.trip_id_to_idx_.push_back(im.map(i));
      }
      for (auto i : new_trip_ids) {
        auto entry = tt.trip_ids_.emplace_back();
        for (auto j : i) {
          auto trip_id = trip_id_idx_t{im.map(j)};
          entry.emplace_back(trip_id);
        }
      }
      for (auto i : new_trip_id_src) {
        tt.trip_id_src_.push_back(im.map(i));
      }
      for (auto i : new_trip_id_strings) {
        tt.trip_id_strings_.emplace_back(i);
      }
      //tt.trip_train_nr_ not used during loading
      assert(tt.trip_train_nr_.size() == 0);
      /* 	 trip_idx_t	 */
      auto add_size = trip_idx_t{new_trip_direction_id.size()};
      tt.trip_direction_id_.resize(to_idx(im.map(add_size)));
      for (auto i = trip_idx_t{0U}; i < add_size; ++i) {
        tt.trip_direction_id_.set(im.map(i), new_trip_direction_id.test(i));
      }
      for (auto i : new_trip_route_id) {
        tt.trip_route_id_.push_back(i);
      }
      for (auto i : new_trip_transport_ranges) {
        tt.trip_transport_ranges_.emplace_back(i);
      }
      for (auto i : new_trip_stop_seq_numbers) {
        tt.trip_stop_seq_numbers_.emplace_back(i);
      }
      for (auto i : new_trip_short_names) {
        tt.trip_stop_seq_numbers_.emplace_back(i);
      }
      for (auto i : new_trip_display_names) {
        tt.trip_display_names_.emplace_back(i);
      }
      for (auto i : new_merged_trips) {
        auto vec = tt.merged_trips_.add_back_sized(0U);
        for (auto j : i) {
          vec.push_back(im.map(j));
        }
      }
      /*      route_id_idx_t	 */
      for (auto i : new_route_ids) {
        auto vec = paged_vecvec<route_id_idx_t, trip_idx_t>{};
        for (auto j = route_id_idx_t{0U}; j < i.route_id_trips_.size(); ++j) {
          vec.emplace_back_empty();
          for (auto k : i.route_id_trips_[j]) {
            vec.back().push_back(im.map(k));
          }
        }
        auto providers = vector_map<route_id_idx_t, provider_idx_t>{};
        for (auto j : i.route_id_provider_) {
          providers.push_back(im.map(j));
        }
        i.route_id_trips_ = vec;
        i.route_id_provider_ = providers;
        tt.route_ids_.push_back(i);
      }
      /*     transport_idx_t	*/
      for (auto i : new_transport_first_dep_offset) {
        tt.transport_first_dep_offset_.push_back(i);
      }
      //tt.initial_day_offset_ not used during loading
      assert(tt.initial_day_offset_.size() == 0);
      for (auto i : new_transport_traffic_days) {
        tt.transport_traffic_days_.push_back(corrected_indices[bitfield_idx_t{i}]);
      }
      for (auto i : new_transport_to_trip_section) {
        auto vec = tt.transport_to_trip_section_.add_back_sized(0U);
        for (auto j : i) {
          vec.push_back(im.map(j));
        }
      }
      for (auto i : new_transport_section_attributes) {
        tt.transport_section_attributes_.emplace_back(i);
      }
      for (auto i : new_transport_section_providers) {
        auto vec = tt.transport_section_providers_.add_back_sized(0U);
        for (auto j : i) {
          vec.push_back(im.map(j));
        }
      }
      for (auto i : new_transport_section_directions) {
        tt.transport_section_directions_.emplace_back(i);
      }
      auto const trip_lines_offset = trip_line_idx_t{tt.trip_lines_.size()};
      for (auto i : new_transport_section_lines) {
        auto vec = tt.transport_section_lines_.add_back_sized(0U);
        for (auto j : i) {
          vec.push_back(j + trip_lines_offset);
        }
      }
      for (auto i : new_transport_section_route_colors) {
        tt.transport_section_route_colors_.emplace_back(i);
      }
      /*        Meta infos	*/
      for (auto i : new_trip_lines) {
        tt.trip_lines_.emplace_back(i);
      }
      /*        area_idx_t	*/
      for (auto i : new_areas) {
        tt.areas_.push_back(area{string_map[i.id_], string_map[i.name_]});
      }
      /*      attribute_idx_t	*/
      auto const attribute_idx_offset = attribute_idx_t{tt.attributes_.size()};
      for (auto i : new_attributes) {
        tt.attributes_.push_back(i);
      }
      for (auto i : new_attribute_combinations) {
        auto vec = tt.attribute_combinations_.add_back_sized(0U);
        for (auto j : i) {
          vec.push_back(j + attribute_idx_offset);
        }
      }
      /*  trip_direction_string_idx_t	*/
      for (auto i : new_trip_direction_strings) {
        tt.trip_direction_strings_.emplace_back(i);
      }
      for (auto i : new_trip_directions) {
        tt.trip_directions_.push_back(im.map(i));
      }
      /*     Other	*/
      //tt.profiles_ not used during loading
      assert(tt.profiles_.size() == 0);
      //tt.date_range_ not changed
      assert(tt.date_range_ == date_range);
      /* Save snapshot */
      fs::create_directories(local_cache_path);
      if (shapes != nullptr) {
          shapes->add(shape_store.get());
          shape_store = std::make_unique<shapes_storage>(local_cache_path, shapes->mode_);
          shape_store->add(shapes);
      }
      tt.write(local_cache_path / "tt.bin");
      progress_tracker->context("");
    } else if (!ignore) {
      throw utl::fail("no loader for {} found", path);
    } else {
      log(log_lvl::error, "loader.load", "no loader for {} found", path);
    }
  }

  finalize(tt, finalize_opt);

  return tt;
}

}  // namespace nigiri::loader
