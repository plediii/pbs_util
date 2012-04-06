
var PBSMON;

var update_timer_callback = null;
var update_timer = null;

var render_timer_callback = null;
var render_timer = null;

var pbsmon_module = function () 
{
    var pbsmon_model = function () 
    {
	var ready = false;
	var ready_callback=null;

	var try_ready = function () 
	{
	    if (ready_callback && ready) 
	    {
		ready_callback();
	    }
	};

	var job_infos = []; 	// the complete list of jobs sent by the server
	var hosts = []; 	// hosts seen by the server
	var filter_text = "";

	var watched_filters = [];

	var new_watched_filter = function (filter_text, filter_func)
	// takes a func transforming a list into a list, and a name
	// describing the transformation, creating a watch filter
	// object
	{
	    return {name: filter_text,
		    running_count: function ()
		    {
			var filtered_job_infos = filter_func(job_infos);
			var count;
			var filtered_job_info;

			count = 0;

			for (idx in filtered_job_infos)
			{
			    filtered_job_info = filtered_job_infos[idx];
			    if (filtered_job_info.elapsed_time !== '--')
			    {
				count = count+ 1;
			    }
			}
			return count;
		    },
		    count: function ()
		    {
			var filtered_job_infos = filter_func(job_infos);
			return filtered_job_infos.length;
		    },
		    set_filter: function ()
		    {
			set_filter_text(filter_text);
		    }};
	};

	var new_filter_function = function (filter_text)
	// createa a new filter function (list to list) filtering for
	// the given text in one of the fields
	{
	    if (!filter_text)
	    {
		return null;
	    }

	    var filter_texts = filter_text.split(' ');

	    var filter_function = function (job_infos)
	    {
		var filtered_job_infos = [];
		var job_info;
		var filter_text_part;
		var all_match;

		for (idx in job_infos) 
		{
		    job_info = job_infos[idx];
		    job_info_str = 'hostname:' + job_info['hostname'] + ' username:' + job_info['username'] + ' jobid:' + job_info['jobid'] + ' jobname:' + job_info['jobname'] + ' elapsedtime:' + job_info['elapsed_time'];


		    all_match = true;
		    for (idx in filter_texts)
		    {
			filter_text_part = filter_texts[idx];
			
			if (filter_text_part)
			{
			    if (!job_info_str.match(filter_text_part))
			    {
				all_match = false;
				break;
			    }
			}
		    }

		    if (all_match) 
		    {
			filtered_job_infos.push(job_info);
		    }
		}
		return filtered_job_infos;
	    };
	    return filter_function;
	};


	var set_filter_text = function (new_text)
	{
	    filter_text = new_text;
	};

	var update_job_infos = function (callback)
	{
	    $.ajax({url: 'get_state',
		    success: function (new_job_infos) 
		    {
			job_infos = new_job_infos;

			if (callback) 
			{
			    callback(job_infos);
			}
		    }});
	};


	var update_hosts = function (callback)
	{
	    $.ajax({url: 'get_hosts',
		    success: function (new_hosts) 
		    {
			hosts = new_hosts

			if (callback) 
			{
			    callback(hosts);
			}
		    }});
	};

	var watched_filters_local_storage_name = 'pbsmon_watched_filter_texts_json';

	var model_version = 1;
	var model_version_local_storage_name = 'pbsmon_model_version_json';

	var retrieve_watched_filters = function ()
	// Try to pull the old list of watched filters from localStorage
	{

	    var retrieved_watched_filters = [];
	    var bad_version = true;

	    if ('localStorage' in window)
	    {

		if (window.localStorage[model_version_local_storage_name])
		{
		    stored_version = JSON.parse(window.localStorage[model_version_local_storage_name]);
		    if (stored_version === model_version)
		    {
			bad_version = false;
		    }
		}
		
		if (bad_version)
		{
		    window.localStorage[watched_filters_local_storage_name] = null;
		}

		watched_filter_texts_json = window.localStorage[watched_filters_local_storage_name];
		if (watched_filter_texts_json)
		{
		    
		    watched_filter_texts = JSON.parse(watched_filter_texts_json);

		    if (watched_filter_texts)
		    {
			var idx;
			for (idx in watched_filter_texts)
			{
			    watched_filter_text = watched_filter_texts[idx];
			    retrieved_watched_filters.push(new_watched_filter(watched_filter_text, new_filter_function(watched_filter_text)));
			}
		    }
		    
		}
	    }
	    else
	    {
		console.log("no local storage");
	    }

	    return retrieved_watched_filters;
	};

	var store_watched_filters = function (watched_filters)
	{
	    if ('localStorage' in window)
	    {
		watched_filter_texts = [];
		for (idx in watched_filters)
		{
		    watched_filter = watched_filters[idx];
		    watched_filter_texts.push(watched_filter.name);
		}

		watched_filter_texts_json = JSON.stringify(watched_filter_texts);
		window.localStorage[watched_filters_local_storage_name] = watched_filter_texts_json;
		window.localStorage[model_version_local_storage_name] = JSON.stringify(model_version);
	    }
	};

	watched_filters = retrieve_watched_filters();

	if (watched_filters && watched_filters.length > 0)
	{
	    watched_filters[0].set_filter();
	}

	// $(function () { retrieve_watched_filters(); });

	ready = true;
	// update_job_infos(function () 
	// 		 {
	// 		     ready = true;
	// 		     try_ready();
	// 		 });

	var get_current_filter_function = function ()
	{
	    return new_filter_function(filter_text);
	};

	return {
	    ready: function (callback) 
	    {
		ready_callback = callback;
		try_ready();
	    },

	    update_job_infos: update_job_infos,

	    update_hosts: update_hosts,

	    get_filter_text: function ()
	    {
		return filter_text;
	    },
	    set_filter_text: set_filter_text, 

	    get_job_infos: function () 
	    {
		return job_infos;
	    },

	    get_hosts: function () 
	    {
		return hosts;
	    },


	    get_watched_filters: function ()
	    {
		return watched_filters;
	    },
	    
	    get_filtered_job_infos: function ()
	    {
		var filter_function = get_current_filter_function();
		
		if (filter_function)
		{
		    return filter_function(job_infos);
		}
		else
		{
		    return job_infos;
		}		
	    },
	    
	    add_watch: function ()
	    {
		watched_filters.push(new_watched_filter(filter_text, get_current_filter_function()));

		store_watched_filters(watched_filters);
	    },

	    add_to_filter_text: function (text)
	    {
		filter_text = filter_text.trim() + ' ' + text;
	    },

	    remove_watched_filter: function (name)
	    {
		var new_watched_filters = [];
		var watched_filter;
		var idx;

		for (idx in watched_filters)
		{
		    watched_filter = watched_filters[idx];
		    
		    if (watched_filter.name !== name)
		    {
			new_watched_filters.push(watched_filter);
		    }
		}
		watched_filters = new_watched_filters;

		store_watched_filters(watched_filters);
	    }
	};
    };

    var pbsmon_view = function () 
    {
	var controller;
	var model;

	var ready_callback = null;
	var ready = false;
	
	var try_ready = function () 
	{
	    if (on_ready_callback && ready) 
	    {
		on_ready_callback();
	    }
	};


	var tbody_selection;
	var filter_text;
	var watch_button;
	var watched_tbody;
	var note_update;

	var link_search_controller = function ()
	// Attach the controller functions to the DOM elements
	{
	    filter_text.keyup(function () {
		controller.on_filter_text_keyup($(this).val());
	    });

	    watch_button.click(function ()
			       {
				   controller.on_watch_button_click();
			       });

	    note_update.click(controller.on_note_update_click);
	};

	$(function ()
	  {
	      tbody_selection = $("#jobinfos");
	      watched_tbody = $("#watching");
	      filter_text = $("#filtertext");

	      watch_button = $("#watch");
	      watch_table = $("#watching");

	      note_update = $("#noteupdate");
	      hosts_div = $("#hosts");

	      ready = true;
	      try_ready();

	      if (controller)
	      {
		  link_search_controller();
	      }
	  });

	var job_info_tr = function (job_info)
	{
	    return '<tr><td>' + job_info['hostname'] + '</td> <td> ' + job_info['username'] + '</td> <td>' + job_info['jobid'] + '</td> <td> ' + job_info['jobname'] + '</td> <td>' + job_info['time'] + '</td> </tr>'
	};

	var watched_tr = function (idx, watched_filter)
	{
	    var count = watched_filter.count();

	    if (count === 0)
	    {
		countclass = 'class="alert select"';
	    }
	    else
	    {
		countclass='class="select"';
	    }
	    return '<tr id="watch' + idx + '"> <td id="name" class="select">' + watched_filter.name + ' </td> <td id="count"' + countclass + ' > ' + watched_filter.running_count() + '/' + count + '</td> <td class="kill"> X </td> </tr> ';
	};

	var render_table = function ()
	{
	    var job_infos = model.get_filtered_job_infos();
	    var idx;
	    var job_info;
	    var nothing = true;

	    tbody_selection.empty();
	    $("tfoot", tbody_selection.parent()).remove();

	    var job_info_trs = [];

	    for (idx in job_infos)
	    {
		job_info = job_infos[idx];
		
		job_info_trs.push(job_info_tr(job_info));
		nothing = false;
	    }

	    if (nothing)
	    {
		tbody_selection.parent().append('<tfoot> <tr> <td colspan="5"> <center> Nothing. </center> </td> </tr> </tfoot>');
	    }
	    else
	    {
		tbody_selection.append(job_info_trs.join(''));

		$("tr:even", tbody_selection).addClass('alternate');

		$("td", tbody_selection).click(function ()
					       {
						   controller.on_filterable_click($(this).text().trim());
					       });
	    }
	    

	};

	var fix_filter_text = function ()
	{
	    filter_text.val(model.get_filter_text());
	};

	return {

	    ready: function (callback) 
	    {
		on_ready_callback = callback;
		try_ready();
	    },

	    set_cm: function (new_controller, new_model) 
	    {
		controller = new_controller;
		model = new_model;

		if (ready)
		{
		    link_search_controller();
		}
	    },

	    fix_filter_text: fix_filter_text,

	    render_table: render_table, 

	    render_watched: function ()
	    {
		var watched_filters = model.get_watched_filters();
		var watched_filter;
		var idx;
	
		watched_tbody.empty();
		for (idx in watched_filters)
		{
		    watched_filter = watched_filters[idx];
		    watched_tbody.append(watched_tr(idx, watched_filter));
		}

		idx = 0;
		var s = $("tr", watched_tbody);

		for (idx in watched_filters)
		{
		    $("#watch" + idx + " td.select").click(controller.on_watched_click_callback(watched_filters, idx));
		    $("#watch" + idx + " td.kill").click(controller.on_kill_watched_click_callback(watched_filters, idx));
		}
		
	    },

	    render_hosts: function ()
	    {
		var hosts = model.get_hosts();

		hosts_div.empty()

		if (hosts.length > 0) 
		{
		    var host_span_text = '<span>' + hosts.join('</span>, <span>') + '</span>';
		    hosts_div.html(host_span_text);

		    $("span", hosts_div).each(function () 
					      {
						  var text = $(this).text();
						  $(this).click(function () 
								{
								    controller.on_filterable_click(text);
								});
					      });
		    
		}

	    },
	    
	    enable_watch: function ()
	    {
		watch_button.removeClass("hidden");
		$(".watchbutton").removeClass("clicked");
	    },
	    disable_watch: function ()
	    {
		watch_button.addClass("hidden");
	    },
	    show_watch_button_click: function ()
	    {
		$(".watchbutton").addClass("clicked");
	    },

	    note_update: function ()
	    {
		note_update.text('' + new Date());
		note_update.addClass('updating');
	    },

	    note_updated: function ()
	    {
		note_update.removeClass('updating');
	    }
	}
    };

    var pbsmon_controller = function (model, view) 
    {

	model_ready = false;
	view_ready = false;


	var on_filtered_job_infos_change = function ()
	{
	    view.render_table();

	    var filter_text = model.get_filter_text();
	    var watched_filters = model.get_watched_filters();
	    var watched_filter;

	    if (filter_text)
	    {
		var new_filter = true;
		if (model.get_filtered_job_infos().length > 0)
		{
		    var idx;
		    
		    for (idx in watched_filters)
		    {
			watched_filter = watched_filters[idx];
			if (filter_text === watched_filter.name)
			{
			    new_filter = false;
			    break;
			}
		    }
		}
		

		if (new_filter)
		{
		    view.enable_watch();
		}
		else
		{
		    view.disable_watch();
		}
	    }
	    else
	    {
		view.disable_watch();
	    }
	};

	var on_job_infos_change = function ()
	{
	    view.render_watched();
	};

	var on_hosts_change = function ()
	{
	    view.render_hosts();
	};


	var update_pbsmon = function update_pbsmon ()
	{
	    var old_filtered_job_infos = model.get_filtered_job_infos();
	    var old_job_infos = model.get_job_infos();

	    model.update_job_infos(function () 
				   {
				       console.log('TODO:these array comparisons always evalutate to false; fix it to do what I intend');
				       if (old_filtered_job_infos !== model.get_filtered_job_infos())
				       {
					   on_filtered_job_infos_change();
				       }

				       if (old_job_infos !== model.get_job_infos())
				       {
					   on_job_infos_change();
				       }
				       view.note_updated();
				   });

	    model.update_hosts(function () 
				   {
				       on_hosts_change();
				   });

	    view.note_update();
	};

	var try_ready = function () 
	{
	    if (model_ready && view_ready) 
	    {
		fix_update_timer_callback();
		view.fix_filter_text();
	    }
	};

	model.ready(function () 
		    {
			model_ready = true;
			try_ready();
		    });

	view.ready(function () 
		   {
		       view_ready = true;
		       try_ready();
		   });


	var clear_update_timer = function ()
	{
	    // here we are setting the update_timer at the global scope
	    if (update_timer)
	    {
		clearTimeout(update_timer);
		update_timer = null;
	    }
	}

	var fix_update_timer_callback = function ()
	{
	    // here we are setting the update_timer at the global scope.  I think this is required in order for the timer to work.
	    var update_time = 60000;  // 60 000 ms == 60s
	    clear_update_timer();

	    update_timer_callback = function ()
	    {
		update_pbsmon();

		update_timer = setTimeout("update_timer_callback()", update_time);
	    }

	    update_timer_callback();
	};

	var clear_render_timer = function ()
	{
	    if (render_timer)
	    {
		clearTimeout(render_timer);
		render_timer = null;
	    }
	};

	var fix_render_timer_callback = function ()
	{
	    clear_render_timer();
	    
	    render_timer_callback = function ()
	    {
		on_filter_text_change();
	    }
	    render_timer = setTimeout("render_timer_callback()", 500); // 500 ms == 0.5s
	};

	var on_filter_text_change = function ()
	{
	    on_filtered_job_infos_change();
	};

	return {

	    on_filter_text_keyup: function (text)
	    // called after the user finishes clicking the keyboard in the filter textbox
	    {
		var old_job_infos = model.get_filtered_job_infos();
		model.set_filter_text(text);

		fix_render_timer_callback();
		
	    },

	    on_watch_button_click: function ()
	    {
		view.show_watch_button_click();
		model.add_watch();
		view.render_watched();
		view.disable_watch();
	    },

	    on_note_update_click: function ()
	    // called when the user clicks the datetime text
	    {
		clear_update_timer();
		// TODO: assert the timer is properly set
		update_timer_callback();
	    },

	    on_filterable_click: function (text)
	    // call when the user clicks on an element in the table
	    {
		model.add_to_filter_text(text);
		
		view.fix_filter_text();
		on_filter_text_change();
		view.render_table();
	    },

	    on_watched_click_callback: function (watched_filters, idx)
	    // called when the watch button is clicked
	    {
		return function ()
		{
		    watched_filters[idx].set_filter();
		    view.fix_filter_text();
		    view.render_table();
		}
	    },

	    on_kill_watched_click_callback: function (watched_filters, idx)
	    // called when the kill button on a watch is clicked
	    {
		return function ()
		{
		    model.remove_watched_filter(watched_filters[idx].name);
		    view.render_watched();
		}
	    }
	};
    };

    var MODEL = pbsmon_model();
    var VIEW = null;
    var CONTROLLER = null;


    VIEW = pbsmon_view();
    CONTROLLER = pbsmon_controller(MODEL, VIEW);
    VIEW.set_cm(CONTROLLER, MODEL);

    return {CONTROLLER:CONTROLLER,
	    MODEL:MODEL,
	    VIEW:VIEW}
};

// fix django csrf issues
// $(document).ajaxSend(function(event, xhr, settings) 
// 		     {
// 			 function getCookie(name) 
// 			 {
// 			     var cookieValue = null;
// 			     if (document.cookie && document.cookie != '') 
// 			     {
// 				 var cookies = document.cookie.split(';');
// 				 for (var i = 0; i < cookies.length; i++) 
// 				 {
// 				     var cookie = jQuery.trim(cookies[i]);
// 				     // Does this cookie string begin with the name we want?
// 				     if (cookie.substring(0, name.length + 1) == (name + '=')) 
// 				     {
// 					 cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
// 					 break;
// 				     }
// 				 }
// 			     }
// 			     return cookieValue;
// 			 }
// 			 function sameOrigin(url) 
// 			 {
// 			     // url could be relative or scheme relative or absolute
// 			     var host = document.location.host; // host + port
// 			     var protocol = document.location.protocol;
// 			     var sr_origin = '//' + host;
// 			     var origin = protocol + sr_origin;
// 			     // Allow absolute or scheme relative URLs to same origin
// 			     return (url == origin || url.slice(0, origin.length + 1) == origin + '/') ||
// 				 (url == sr_origin || url.slice(0, sr_origin.length + 1) == sr_origin + '/') ||
// 				 // or any other URL that isn't scheme relative or absolute i.e relative.
// 				 !(/^(\/\/|http:|https:).*/.test(url));
// 			 }
// 			 function safeMethod(method) 
// 			 {
// 			     return (/^(GET|HEAD|OPTIONS|TRACE)$/.test(method));
// 			 }

// 			 if (!safeMethod(settings.type) && sameOrigin(settings.url)) 
// 			 {
// 			     xhr.setRequestHeader("X-CSRFToken", getCookie('csrftoken'));
// 			 }
// 		     });


PBSMON = pbsmon_module();

// $(function() 
//   {
      
//   }); 

