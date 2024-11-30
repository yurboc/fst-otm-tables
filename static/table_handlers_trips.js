// Таблица с походами
var table;
const table_min_height = 300;
const table_max_height = 1230;

// Временная шкала
var timeline_items = new vis.DataSet();
var timeline_groups = new vis.DataSet();
var timeline_options = {};
var timeline;

// Фильтр по дате начала похода
function customFilterByDateStart(data, filterParams){
    //data - the data for the row being filtered
    //filterParams - params object passed to the filter
    var isInFuture = moment(data.start_date, "DD.MM.YYYY") > moment();
    var isInRange = moment(data.start_date, "DD.MM.YYYY").subtract(filterParams.value, filterParams.units) < moment();
    return isInFuture && isInRange;
}

// Фильтр по дате завершения похода
function customFilterByDateFinish(data, filterParams){
    //data - the data for the row being filtered
    //filterParams - params object passed to the filter
    var isInPast = moment(data.finish_date, "DD.MM.YYYY").add(1,'days') < moment();
    var isInRange = moment(data.start_date, "DD.MM.YYYY").add(filterParams.value, filterParams.units) > moment();
    return isInPast && isInRange;
}

// Фильтр по датам похода
function customFilterByDate(data, filterParams){
    //data - the data for the row being filtered
    //filterParams - params object passed to the filter
    switch (filterParams.type) {
        case "past":
            return moment(data.finish_date, "DD.MM.YYYY").add(1,'days') < moment();
        case "present":
            var isStarted = moment(data.start_date, "DD.MM.YYYY") < moment();
            var isFinished = moment(data.finish_date, "DD.MM.YYYY").add(1,'days') < moment();
            return isStarted && !isFinished;
        case "future":
            return moment(data.start_date, "DD.MM.YYYY") > moment();
    }
    return false;
}

// Фильтр по категории сложности
function customFilterByDifficulty(data, filterParams){
    //data - the data for the row being filtered
    //filterParams - params object passed to the filter
    return data.difficulty.startsWith(filterParams.difficulty);
}

// Применить фильтры
function applyFilters() {
    var complex_filter = [];
    var new_url_data = {};

    // Фильтр по имени
    if (document.getElementById("find_name_str").value != "") {
        complex_filter.push({field:"name", type:"like", value:document.getElementById("find_name_str").value});
        new_url_data["find_name_str"] = document.getElementById("find_name_str").value;
    }

    // Фильтр по клубу
    if (document.getElementById("find_club_str").value != "") {
        complex_filter.push({field:"club", type:"like", value:document.getElementById("find_club_str").value});
        new_url_data["find_club_str"] = document.getElementById("find_club_str").value;
    }

    // Фильтр по виду спорта (тип "маршрут")
    if (document.getElementById("sport_type").value != "все") {
        complex_filter.push({field:"sport_type", type:"like", value:document.getElementById("sport_type").value});
        new_url_data["sport_type"] = document.getElementById("sport_type").value;
    }

    // Применение фильтров
    table.setFilter(complex_filter);

    // Дополнительная фильтрация по категориям сложности
    if (document.getElementById("difficulty").value != "все") {
        table.addFilter(customFilterByDifficulty, {difficulty:document.getElementById("difficulty").value});
        new_url_data["difficulty"] = document.getElementById("difficulty").value;
    }

    // Дополнительная фильтрация по датам
    switch ($('#date').val()) {
        case "past":
            table.addFilter(customFilterByDate, {type:"past"});
            break;
        case "present":
            table.addFilter(customFilterByDate, {type:"present"});
            break;
        case "future":
            table.addFilter(customFilterByDate, {type:"future"});
            break;
        case "next1month":
            table.addFilter(customFilterByDateStart, {units:"months", value:1});
            break;
        case "next2month":
            table.addFilter(customFilterByDateStart, {units:"months", value:2});
            break;
        case "next6month":
            table.addFilter(customFilterByDateStart, {units:"months", value:6});
            break;
        case "prev1month":
            table.addFilter(customFilterByDateFinish, {units:"months", value:1});
            break;
        case "prev2month":
            table.addFilter(customFilterByDateFinish, {units:"months", value:2});
            break;
        case "prev6month":
            table.addFilter(customFilterByDateFinish, {units:"months", value:6});
            break;
    }
    if ($('#date').val() != "все") {
        new_url_data["date"] = $('#date').val();
    }

    // Печать количества записей
    $('#search-results').html("Найдено записей: <strong>" + table.getDataCount('active') + "</strong>");

    // Обновление графика
    if ($('#visualization').is(":visible")) {
        updateVisualization();
        timeline.fit();
    }

    // Снять выделение в таблице
    table.deselectRow();

    // Обновление строки URL
    const searchParams = new URLSearchParams(new_url_data);
    history.replaceState(null, document.title, window.location.pathname + (Object.keys(new_url_data).length ? "?" : "") + encodeURI(searchParams.toString()))
}

// Очистить все фильтры
function doFilterClear() {
    $('#find_name_str').val('');
    $('#find_club_str').val('');
    $('#sport_type').val('все').selectmenu('refresh');
    $('#difficulty').val('все').selectmenu('refresh');
    $('#date').val('все').selectmenu('refresh');
    applyFilters();
}

// Показать/скрыть визуализацию графика походов
function toggleVisualization() {
    if ($('#visualization').is(":visible")) {
      // Скрыть
      $('#visualization').hide();
      table.setHeight(table_max_height);
    }
    else {
      // Показать
      $('#visualization').show();
      table.setHeight(table_min_height);
      // Обновить
      updateVisualization();
      timeline.fit();
      // Снять выделение
      table.deselectRow();
    }
}

// Обновление визуализации графика походов
function updateVisualization() {
    var items = [];
    var groups = [];
    var groups_list = [];
    var table_data = table.getData("active");

    // Формирование элементов (походы)
    table_data.forEach(function(item) {
      var rowItems = {
          id:      item.id,
          content: item.name  + " (" + item.part_cnt + " чел.) - " + item.club + "; к.с. " + item.difficulty + "; " + item.region,
          start:   moment(item.start_date, "DD.MM.YYYY").format(),
          end:     moment(item.finish_date, "DD.MM.YYYY").add(1,'days').format(),
          group:   item.sport_type
      };
      items.push(rowItems);
      if (!groups_list.includes(item.sport_type)) {
          groups_list.push(item.sport_type);
      }
    })

    // Формирование групп (виды спорта)
    groups_list.forEach(function(item) {
        var groupItems = {
          id: item,
          content: item
        };
        groups.push(groupItems);
    })

    // Заполнение графика
    timeline_groups.clear();
    timeline_groups.add(groups);
    timeline_items.clear();
    timeline_items.add(items);
    timeline.setGroups(timeline_groups);
    timeline.setItems(timeline_items);

    // Снятие выделения на графике
    timeline.setSelection([]);
}

$( function() {

    // Создание объекта Tabulator на DOM элементе с идентификатором "full-table"
    table = new Tabulator("#full-table", {
        height:table_min_height,
        tooltips:true,
        data:php_data, // assign data to table
        layout:"fitData", // fit columns to width of table (optional)
        selectable:1,
        columns:[ // Define Table Columns
            {title:"№<br/>п/п",                       field:"id",          width:50, tooltip: false},
            {title:"№<br/>МК",                        field:"book_id",     width:70  },
            {title:"Фамилия И.О.<br/>руководителя",   field:"name",        width:150 },
            {title:"Организация",                     field:"club",        width:150 },
            {title:"Кол-во<br/>человек",              field:"part_cnt",    width:50  },
            {title:"Вид<br/>маршрута",                field:"sport_type",  width:130 },
            {title:"План.<br/>к.с.",                  field:"difficulty",  width:70  },
            {title:"Туристский<br/>район",            field:"region",      width:170 },
            {title:"План.<br/>старт",                 field:"start_date",  width:100, formatter:"datetime", formatterParams:{inputFormat:"DD.MM.YYYY",outputFormat:"DD.MM.YYYY"}, sorter:"date", sorterParams:{format:"DD.MM.YYYY"},
                formatter: function(cell, formatterParams){
                    var start_date_value = cell.getValue();
                    var finish_date_value = cell.getRow().getData().finish_date;
                    var is_ongoing = (moment(start_date_value, "DD.MM.YYYY") < moment()) && (moment() < moment(finish_date_value, "DD.MM.YYYY").add(1,'days'));
                    if (is_ongoing) {
                      return "<span style='color:green; font-weight:bold;'>" + start_date_value + "</span>";
                    }
                    else if((moment(start_date_value, "DD.MM.YYYY") > moment()) && (moment(start_date_value, "DD.MM.YYYY").subtract(1,'month') < moment())) {
                      return "<span style='color:teal; font-weight:bold;'>" + start_date_value + "</span>";
                    }
                    else {
                      return "<span style='font-weight:bold;'>" + start_date_value + "</span>";
                    }
                }
            },
            {title:"План.<br/>финиш",                 field:"finish_date", width:100, formatter:"datetime", formatterParams:{inputFormat:"DD.MM.YYYY",outputFormat:"DD.MM.YYYY"}, sorter:"date", sorterParams:{format:"DD.MM.YYYY"},
                formatter: function(cell, formatterParams){
                    var start_date_value = cell.getRow().getData().start_date;
                    var finish_date_value = cell.getValue();
                    var is_ongoing = (moment(start_date_value, "DD.MM.YYYY") < moment()) && (moment() < moment(finish_date_value, "DD.MM.YYYY").add(1,'days'));
                    if (is_ongoing) {
                      return "<span style='color:green; font-weight:bold;'>" + finish_date_value + "</span>";
                    }
                    else if((moment(finish_date_value, "DD.MM.YYYY").add(1,'days') < moment()) && (moment(finish_date_value, "DD.MM.YYYY").add(1,'days').add(1,'month') > moment())) {
                      return "<span style='color:orange; font-weight:bold;'>" + finish_date_value + "</span>";
                    }
                    else {
                      return "<span style='font-weight:bold;'>" + finish_date_value + "</span>";
                    }
                }
            },
            {title:"Председатель<br/>МКК",            field:"mkk_leader",  width:135 },
            {title:"Члены<br/>МКК",                   field:"mkk_list",    width:135 },
        ],
        rowClick:function(e, row){
            //e - the click event object
            //row - row component
            timeline.setSelection(row.getData().id);
            timeline.focus(row.getData().id);
        }
    });

    // Применение URL-параметров
    if (window.location.search.length) {
        const urlParams = new URLSearchParams(decodeURI(window.location.search));
        urlParams.forEach(function(value, key) {
            $("#"+key).val(value);
        });
    }

    // Выбор вида спорта (тип "маршрут")
    $( "#sport_type" ).selectmenu({
        change: function( event, data ) {
            // Значение можно достать из data.item.value
            applyFilters();
        }
    });

    // Выбор планируемой категории сложности
    $( "#difficulty" ).selectmenu({
        change: function( event, data ) {
            // Значение можно достать из data.item.value
            applyFilters();
        }
    });

    // Выбор фильтра по датам
    $( "#date" ).selectmenu({
        change: function( event, data ) {
            // Значение можно достать из data.item.value
            applyFilters();
        }
    });

    // Кнопки "Найти", "Сброс" и "Визуализация"
    $( "#find_btn" ).button().click(applyFilters);
    $( "#clear_btn" ).button().click(doFilterClear);
    $( "#visualization_btn" ).button().click(toggleVisualization);


    // Информация о табличке
    $( "#dialog_about" ).dialog({
        autoOpen: false,
        width: 495,
        modal: true,
        buttons: {
            OK: function() {
                $( this ).dialog( "close" );
            }
        },
        position: {
            my: "center top",
            at: "center top",
            of: "#full-table", // of: window, // при window диалог съезжал вниз
            collision: "none"
        },
        //create: function (event, ui) { // чтобы диалог не двигался при прокрутке
        //  $(event.target).parent().css('position', 'fixed');
        //}
    });
    $( "#show_info_btn" ).button( {
        icon: "ui-icon-info",
        showLabel: false
      } ).click(function() {
        $('#dialog_about').dialog('open');
    });

    // Информация о датах создания и модификации
    $( "#modified_date_placeholder" ).text(modified_date);
    $( "#generated_date_placeholder" ).text(generated_date);

    //
    // Подготовка визуализации
    //
    const timeline_container = document.getElementById("visualization");
    timeline = new vis.Timeline(timeline_container, timeline_items, timeline_options);
    updateVisualization();
    timeline.setWindow(moment().subtract(15,'days').format(), moment().add(15,'days').format());
    timeline.on('select', function (properties) {
      table.deselectRow();
      if (properties.items.length) {
        table.selectRow(properties.items[0]);
        table.scrollToRow(properties.items[0], "top");
      }
    });

    // Обновление при наличии URL-параметров
    if (window.location.search.length) {
      applyFilters();
    }
});
