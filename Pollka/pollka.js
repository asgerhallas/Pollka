(function ($) {
    if (!$) throw new Error('JQuery is required');

    $.endpoint = {
        clientId: this.generateGuid(),
        subscriptions: [],
        retryDelay: 50
    };

    $.endpoint.connect = function() {
        
    };

    $.endpoint.connect = function () {
        if ($.endpoint.connectionId == 0)
            throw new Error('Not negotiated.');

        $.ajax('/poll/connect',
			{
			    type: "POST",
			    data: {
			        connectionId: $.endpoint.connectionId
			    },
			    success: function (messages) {
			        $.endpoint.connect();

			        for (var j = 0; j < messages.length; j++) {
			            var message = messages[j];
			            var handlers = $.endpoint.handlers[message.channel + '_' + message.group];
			            if (typeof handlers !== 'undefined') {
			                for (var i = 0; i < handlers.length; i++) {
			                    handlers[i](message);
			                }
			            }
			        }
			    },
			    error: function () {
			        setTimeout(function () {
			            $.endpoint.connect();
			        }, $.endpoint.retryDelay);
			    }
			});
    };

    $.endpoint.subscribe = function (channel, group, handler) {
        if ($.endpoint.connectionId == 0) {
            $.endpoint.pendingSubscriptions.push(arguments);
            return;
        }
    };
})(jQuery);
