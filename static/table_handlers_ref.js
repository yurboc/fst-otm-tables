// Таблица с судьями
var table;

// Фильтр по дате присвоения разряда
function customFilterByDateApply(data, filterParams){
    //data - the data for the row being filtered
    //filterParams - params object passed to the filter
    return moment(data.date_apply, "DD.MM.YYYY") >= moment().subtract(filterParams.months, 'months');
}

// Фильтр по дате истечения разряда
function customFilterByDateExpire(data, filterParams){
    //data - the data for the row being filtered
    //filterParams - params object passed to the filter
    var expired = moment(data.date_expire, "DD.MM.YYYY") < moment();
    if (typeof filterParams.filterExpired !== 'undefined') {
        return (filterParams.filterExpired == 1 && expired) || (filterParams.filterExpired == 0 && !expired);
    }
    var showExpired = (filterParams.value == 0) && expired;
    var suitable = moment(data.date_expire, "DD.MM.YYYY") <= moment().add(filterParams.value, filterParams.units);
    var showSuitable = (filterParams.value != 0) && (!expired) && suitable;
    return showExpired || showSuitable;
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

    // Фильтр по региону
    if (document.getElementById("find_region_str").value != "") {
        complex_filter.push({field:"region", type:"like", value:document.getElementById("find_region_str").value});
        new_url_data["find_region_str"] = document.getElementById("find_region_str").value;
    }

    // Фильтр по виду "дистанция"
    if (document.getElementById("dist_sport").value != "все") {
        if (document.getElementById("dist_sport").value == "отс") {
            complex_filter.push({field:"dist_type", type:"=", value:""});
        }
        else if (document.getElementById("dist_sport").value == "име") {
            complex_filter.push({field:"dist_type", type:"!=", value:""});
        }
        else {
            complex_filter.push({field:"dist_type", type:"like", value:document.getElementById("dist_sport").value});
            complex_filter.push({field:"dist_type", type:"!=", value:""});
        }
        new_url_data["dist_sport"] = document.getElementById("dist_sport").value;
    }

    // Фильтр по виду "маршрут"
    if (document.getElementById("route_sport").value != "все") {
        if (document.getElementById("route_sport").value == "отс") {
            complex_filter.push({field:"route_type", type:"=", value:""});
        }
        else if (document.getElementById("route_sport").value == "име") {
            complex_filter.push({field:"route_type", type:"!=", value:""});
        }
        else {
            complex_filter.push({field:"route_type", type:"like", value:document.getElementById("route_sport").value});
            complex_filter.push({field:"route_type", type:"!=", value:""});
        }
        new_url_data["route_sport"] = document.getElementById("route_sport").value;
    }

    // Фильтр по званиям
    if (document.getElementById("rank").value != "все") {
        if (document.getElementById("rank").value == "СС2К+") {
            complex_filter.push([
                {field:"rank", type:"=", value:"СС2К"},
                {field:"rank", type:"=", value:"СС1К"},
                {field:"rank", type:"=", value:"ССВК"},
            ]);
        }
        else if (document.getElementById("rank").value == "СС1К+") {
            complex_filter.push([
                {field:"rank", type:"=", value:"СС1К"},
                {field:"rank", type:"=", value:"ССВК"},
            ]);
        }
        else {
            complex_filter.push({field:"rank", type:"=", value:document.getElementById("rank").value});
        }
        new_url_data["rank"] = document.getElementById("rank").value;
    }

    // Применение фильтров
    table.setFilter(complex_filter);

    // Дополнительная фильтрация по датам
    switch ($('#date').val()) {
        case "last1month":
            table.addFilter(customFilterByDateApply, {months:1});
            break;
        case "last2month":
            table.addFilter(customFilterByDateApply, {months:2});
            break;
        case "last6month":
            table.addFilter(customFilterByDateApply, {months:6});
            break;
        case "expNo":
            table.addFilter(customFilterByDateExpire, {filterExpired:0});
            break;
        case "expYes":
            table.addFilter(customFilterByDateExpire, {filterExpired:1});
            break;
        case "exp1month":
            table.addFilter(customFilterByDateExpire, {units:"weeks", value:4});
            break;
        case "exp3month":
            table.addFilter(customFilterByDateExpire, {units:"months", value:3});
            break;
    }
    if ($('#date').val() != "все") {
        new_url_data["date"] = $('#date').val();
    }

    // Печать количества записей
    $('#search-results').html("Найдено записей: <strong>" + table.getDataCount('active') + "</strong>");

    // Обновление строки URL
    const searchParams = new URLSearchParams(new_url_data);
    history.replaceState(null, document.title, window.location.pathname + (Object.keys(new_url_data).length ? "?" : "") + encodeURI(searchParams.toString()))
}

// Очистить все фильтры
function doFilterClear() {
    $('#find_name_str').val('');
    $('#find_region_str').val('');
    $('#dist_sport').val('все').selectmenu('refresh');
    $('#route_sport').val('все').selectmenu('refresh');
    $('#rank').val('все').selectmenu('refresh');
    $('#date').val('все').selectmenu('refresh');
    applyFilters();
}

/*
          'id'             => array_key_exists( 0, $row) ? $row[ 0] : "",
          'name'           => array_key_exists( 1, $row) ? $row[ 1] : "",
          'rank'           => array_key_exists( 2, $row) ? $row[ 2] : "",
          'region'         => array_key_exists( 3, $row) ? $row[ 3] : "",
          'rank_state'     => array_key_exists( 4, $row) ? $row[ 4] : "",
          'date_apply'     => array_key_exists( 5, $row) ? $row[ 5] : "",
          'date_expire'    => array_key_exists( 6, $row) ? $row[ 6] : "",
          'order_no'       => array_key_exists( 7, $row) ? $row[ 7] : "",
          'order_issuer'   => array_key_exists( 8, $row) ? $row[ 8] : "",
          'dist_type'      => array_key_exists( 9, $row) ? $row[ 9] : "",
          'route_type'     => array_key_exists(10, $row) ? $row[10] : "",
          'qual_date'      => array_key_exists(11, $row) ? $row[11] : "",
          'statement_link' => array_key_exists(12, $row) ? $row[12] : "",
          'req_app_date'   => array_key_exists(13, $row) ? $row[13] : "",
          'primary_link'   => array_key_exists(14, $row) ? $row[14] : "",
          'order_link'     => array_key_exists(15, $row) ? $row[15] : "",
          'confirm_link'   => array_key_exists(16, $row) ? $row[16] : "",
*/


$( function() {
    // Создание объекта Tabulator на DOM элементе с идентификатором "full-table"
    table = new Tabulator("#full-table", {
        height:1230,
        tooltips:true,
        data:php_data, // assign data to table
        layout:"fitData", // fit columns to width of table (optional)
        columns:[ // Define Table Columns
            {title:"№ п/п", field:"id", width:50, tooltip: false},
            {title:"ФИО судьи", field:"name", width:300},
            {title:"Регион", field:"region", width:100},
            {title:"Судейская категория", field:"rank", width:80},
            {title:"Статус действующей судейской категории", field:"rank_state", width:130},
            {title:"Дата присвоения/подтверждения", field:"date_apply", width:100, formatter:"datetime", formatterParams:{inputFormat:"DD.MM.YYYY",outputFormat:"DD.MM.YYYY"}, sorter:"date", sorterParams:{format:"DD.MM.YYYY"}},
            {title:"Дата, до которой действует категория", field:"date_expire", width:110, formatter:"datetime", formatterParams:{inputFormat:"DD.MM.YYYY",outputFormat:"DD.MM.YYYY"}, sorter:"date", sorterParams:{format:"DD.MM.YYYY"},
                formatter: function(cell, formatterParams){
                    var cell_value = cell.getValue();
                    if(moment(cell_value, "DD.MM.YYYY") <= moment()) {
                      return "<span style='color:red; font-weight:bold;'>" + cell_value + "</span>";
                    }
                    else if(moment(cell_value, "DD.MM.YYYY") <= moment().add(4, 'weeks')) {
                      return "<span style='color:orange; font-weight:bold;'>" + cell_value + "</span>";
                    }
                    else if(moment(cell_value, "DD.MM.YYYY") <= moment().add(3, 'months')) {
                      return "<span style='color:teal; font-weight:bold;'>" + cell_value + "</span>";
                    }
                    else {
                      return "<span style='font-weight:bold;'>" + cell_value + "</span>";
                    }
                }
            },
            {title:"Номер приказа/распоряжения", width:100, field:"order_no",
                formatter: function(cell, formatterParams){
                    var order_no_value = cell.getRow().getData().order_no;
                    var confirm_link_value = cell.getRow().getData().confirm_link;
                    var order_link_value = cell.getRow().getData().order_link;
                    if (confirm_link_value.startsWith("http")) {
                        if (order_no_value == "") {
                            order_no_value = "ссылка";
                        }
                        return "<i class='ui-icon ui-icon-extlink'></i> <a style='color:blue' target='_blank' href='" + confirm_link_value + "'>" + order_no_value + "</a>";
                    }
                    else if (order_link_value.startsWith("http")) {
                        if (order_no_value == "") {
                            order_no_value = "ссылка";
                        }
                        return "<i class='ui-icon ui-icon-extlink'></i> <a style='color:blue' target='_blank' href='" + order_link_value + "'>" + order_no_value + "</a>";
                    }
                    else {
                        return (order_no_value == "") ? "" : "<i class='ui-icon ui-icon-document'></i> " + order_no_value;
                    }
                },
                tooltip: function(cell){
                    //function should return a string for the tooltip of false to hide the tooltip
                    //return cell.getColumn().getField() + " - " + cell.getValue(); //return cells "field - value";
                    var order_no_value = cell.getRow().getData().order_no;
                    var order_link_value = cell.getRow().getData().order_link;
                    var confirm_link_value = cell.getRow().getData().confirm_link;
                    var tooltip_text = "Номер: ";
                    tooltip_text += (order_no_value == "") ? "отсутствует" : order_no_value;
                    if (order_link_value.startsWith("http")) {
                        tooltip_text += "\nСсылка на приказ: " + order_link_value;
                    }
                    if (confirm_link_value.startsWith("http")) {
                        tooltip_text += "\nСсылка на подтверждение: " + confirm_link_value;
                    }
                    if (!order_link_value.startsWith("http") && !confirm_link_value.startsWith("http")) {
                        //return false; // можно вообще не показывать меню
                        tooltip_text += "\nСсылки отсутствуют";
                    }
                    return tooltip_text;
                },
                contextMenu: function(component, e){
                    //component - column/cell/row component that triggered the menu
                    //e - click event object
                    var menu = [];
                    var order_link_value = component.getRow().getData().order_link;
                    var confirm_link_value = component.getRow().getData().confirm_link;
                    if (order_link_value.startsWith("http")) {
                        menu.push({
                            label:"<i class='ui-icon ui-icon-script'></i> Открыть приказ",
                            action:function(e, column){
                                var win = window.open(order_link_value, '_blank');
                                win.focus();
                            }
                        })
                    }
                    if (confirm_link_value.startsWith("http")) {
                        menu.push({
                            label:"<i class='ui-icon ui-icon-check'></i> Открыть подтверждение",
                            action:function(e, column){
                                var win = window.open(confirm_link_value, '_blank');
                                win.focus();
                            }
                        })
                    }
                    if (!order_link_value.startsWith("http") && !confirm_link_value.startsWith("http")) {
                        menu.push({
                            label:"<i class='ui-icon ui-icon-notice'></i> Отсутствуют ссылки на приказы",
                            disabled:true
                        })
                    }
                    menu.push({
                        separator:true,
                    });
                    menu.push({
                        label:"<i class='ui-icon ui-icon-close'></i> Закрыть",
                        action:function(e, column){}
                    });
                    return menu;
                }
            },
            {title:"дистанция", field:"dist_type", width:130},
            {title:"маршрут", field:"route_type", width:130},
            {title:"Ссылка на действующий приказ/подтверждение", field:"primary_link", width:135, visible: false, download: true},
            {title:"Ссылка на действующий приказ", field:"order_link", width:135, visible: false, download: true},
            {title:"Ссылка на действующее подтверждение", field:"confirm_link", width:135, visible: false, download: true},
        ]
    });

    // Применение URL-параметров
    if (window.location.search.length) {
        const urlParams = new URLSearchParams(decodeURI(window.location.search));
        urlParams.forEach(function(value, key) {
            $("#"+key).val(value);
        });
        applyFilters();
    }

    // Выбор вида программы "дистанция"
    $( "#dist_sport" ).selectmenu({
        change: function( event, data ) {
            // Значение можно достать из data.item.value
            applyFilters();
        }
    });

    // Выбор вида программы "маршрут"
    $( "#route_sport" ).selectmenu({
        change: function( event, data ) {
            // Значение можно достать из data.item.value
            applyFilters();
        }
    });

    // Выбор категорий
    $( "#rank" ).selectmenu({
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

    // Кнопки "Найти" и "Сброс"
    $( "#find_btn" ).button().click(applyFilters);
    $( "#clear_btn" ).button().click(doFilterClear);

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

    // Кнопка "Экспорт в XLSX"
    $("#xlsx_btn").button().click(function(){
        table.download("xlsx", "referees.xlsx", {sheetName:"Спортивные судьи"});
    });

    // Информация о датах создания и модификации
    $( "#modified_date_placeholder" ).text(modified_date);
    $( "#generated_date_placeholder" ).text(generated_date);
});
