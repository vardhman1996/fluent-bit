// Turn off ESLint for this file because it's sent down to users as-is.
/* eslint-disable */
window.addEventListener('load', function() {

  // setup apache menu items in nav bar
  /*const community = document.querySelector("a[href='#community']").parentNode;
  const communityMenu =
    '<li>' +
    '<a id="community-menu" href="#">Community</a>' +
    '<div id="community-dropdown" class="hide">' +
      '<ul id="community-dropdown-items">' +
        '<li><a href="/contact">Contant</a></li>' +
        '<li><a href="/events">Events</a></li>' +
        '<li><a href="https://twitter.com/Apache_Pulsar">Twitter</a></li>' +
        '<li><a href="https://github.com/apache/incubator-pulsar/wiki">Wiki</a></li>' +
        '<li><a href="https://github.com/apache/incubator-pulsar/issues">Issue tracking</a></li>' +
        '<li><a href="/resources">Resources</a></li>' +
        '<li><a href="/team">Team</a></li>' +
      '</ul>' +
    '</div>' +
    '</li>';

  community.innerHTML = communityMenu;

  const communityMenuItem = document.getElementById("community-menu");
  const communityDropDown = document.getElementById("community-dropdown");
  communityMenuItem.addEventListener("click", function(event) {
    event.preventDefault();

    if (communityDropDown.className == 'hide') {
      communityDropDown.className = 'visible';
    } else {
      communityDropDown.className = 'hide';
    }
  });*/

  // setup apache menu items in nav bar
  const apache = document.querySelector("a[href='#apache']").parentNode;
  const apacheMenu =
    '<li>' +
    '<a id="apache-menu" href="#">Apache <span style="font-size: 0.75em">&nbsp;▼</span></a>' +
    '<div id="apache-dropdown" class="hide">' +
      '<ul id="apache-dropdown-items">' +
        '<li><a href="https://www.apache.org/" target="_blank" >Foundation &#x2750</a></li>' +
        '<li><a href="https://www.apache.org/licenses/" target="_blank">License &#x2750</a></li>' +
        '<li><a href="https://www.apache.org/foundation/sponsorship.html" target="_blank">Sponsorship &#x2750</a></li>' +
        '<li><a href="https://www.apache.org/foundation/thanks.html" target="_blank">Thanks &#x2750</a></li>' +
        '<li><a href="https://www.apache.org/security" target="_blank">Security &#x2750</a></li>' +
      '</ul>' +
    '</div>' +
    '</li>';

  apache.innerHTML = apacheMenu;

  const apacheMenuItem = document.getElementById("apache-menu");
  const apacheDropDown = document.getElementById("apache-dropdown");
  apacheMenuItem.addEventListener("click", function(event) {
    event.preventDefault();

    if (apacheDropDown.className == 'hide') {
      apacheDropDown.className = 'visible';
    } else {
      apacheDropDown.className = 'hide';
    }
  });


  function button(label, ariaLabel, icon, className) {
    const btn = document.createElement('button');
    btn.classList.add('btnIcon', className);
    btn.setAttribute('type', 'button');
    btn.setAttribute('aria-label', ariaLabel);
    btn.innerHTML =
      '<div class="btnIcon__body">' +
      icon +
      '<strong class="btnIcon__label">' +
      label +
      '</strong>' +
      '</div>';
    return btn;
  }

  function addButtons(codeBlockSelector, btn) {
    document.querySelectorAll(codeBlockSelector).forEach(function(code) {
      code.parentNode.appendChild(btn.cloneNode(true));
    });
  }

  const copyIcon =
    '<svg width="12" height="12" viewBox="340 364 14 15" xmlns="http://www.w3.org/2000/svg"><path fill="currentColor" d="M342 375.974h4v.998h-4v-.998zm5-5.987h-5v.998h5v-.998zm2 2.994v-1.995l-3 2.993 3 2.994v-1.996h5v-1.995h-5zm-4.5-.997H342v.998h2.5v-.997zm-2.5 2.993h2.5v-.998H342v.998zm9 .998h1v1.996c-.016.28-.11.514-.297.702-.187.187-.422.28-.703.296h-10c-.547 0-1-.452-1-.998v-10.976c0-.546.453-.998 1-.998h3c0-1.107.89-1.996 2-1.996 1.11 0 2 .89 2 1.996h3c.547 0 1 .452 1 .998v4.99h-1v-2.995h-10v8.98h10v-1.996zm-9-7.983h8c0-.544-.453-.996-1-.996h-1c-.547 0-1-.453-1-.998 0-.546-.453-.998-1-.998-.547 0-1 .452-1 .998 0 .545-.453.998-1 .998h-1c-.547 0-1 .452-1 .997z" fill-rule="evenodd"/></svg>';

  addButtons(
    '.hljs',
    button('Copy', 'Copy code to clipboard', copyIcon, 'btnClipboard')
  );

  const clipboard = new ClipboardJS('.btnClipboard', {
    target: function(trigger) {
      return trigger.parentNode.querySelector('code');
    },
  });

  clipboard.on('success', function(event) {
    event.clearSelection();
    const textEl = event.trigger.querySelector('.btnIcon__label');
    textEl.textContent = 'Copied';
    setTimeout(function() {
      textEl.textContent = 'Copy';
    }, 2000);
  });

});
